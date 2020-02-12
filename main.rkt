#lang racket/base

(require racket/contract/base)

(provide
 (contract-out
  [fiber? predicate/c]
  [fiber-then (->* (fiber? (-> any/c any/c)) ((-> any/c any/c)) fiber?)]
  [fiber-then-chain (->* (fiber? (-> any/c fiber?)) ((-> any/c fiber?)) fiber?)]
  [fiber-then-call (-> fiber? (-> any/c) fiber?)]
  [fiber-then-call-chaining (-> fiber? (-> fiber?) fiber?)]
  [fiber-combine (-> (unconstrained-domain-> any/c) fiber? ... fiber?)]
  [fiber-combine-chaining
   (-> (unconstrained-domain-> fiber?) fiber? ... fiber?)]
  [fiber-result (-> fiber? (fiber/c result?))]
  [fiber-run-in-background (-> (fiber/c void?) void?)]
  [immediate-fiber (-> any/c fiber?)]
  [immediate-failed-fiber (-> any/c fiber?)]
  [void-fiber (-> (fiber/c void?))]
  [sync-fiber (-> evt? fiber?)]
  [fiber-engine? predicate/c]
  [standard-fiber-engine fiber-engine?]
  [fiber-engine-run (-> fiber-engine? (-> fiber?) fiber?)]
  [fiber-engine-run-evt (-> fiber-engine? (-> fiber?) evt?)]))

(require racket/match
         rebellion/base/option
         rebellion/base/result
         rebellion/collection/list
         rebellion/streaming/reducer
         rebellion/streaming/transducer
         rebellion/type/object)

;@------------------------------------------------------------------------------

(define (fiber? v)
  (or (chained-fiber? v)
      (immediate-fiber? v)
      (external-fiber? v)))

(define-object-type external-fiber (runtime event result-box))
(define-object-type immediate-fiber (runtime result))

(define-object-type chained-fiber
  (runtime input-fiber transformer result-box))

(define-object-type compound-fiber
  (runtime input-fibers readiness-poller combiner result-box))

(define-object-type fiber-runtime-environment
  (identity
   fibers
   background-fibers
   root-fiber))

(define current-fiber-runtime-environment
  (make-parameter #f #f 'current-fiber-runtime-environment))

(define (assert-running-in-fiber-environment who)
  (define runtime (current-fiber-runtime-environment))
  (unless runtime
    (raise-arguments-error who "cannot be called outside of a fiber engine"))
  runtime)

(define (fiber/c success-contract)
  fiber?)

(define (immediate-fiber data)
  (define runtime (assert-running-in-fiber-environment 'immediate-fiber))
  (make-immediate-fiber #:runtime runtime #:result (success data)))

(define (immediate-failed-fiber err)
  (define runtime (assert-running-in-fiber-environment 'immediate-failed-fiber))
  (make-immediate-fiber #:runtime runtime #:result (failure err)))

(define (fiber-then fiber data-transformer [error-handler raise])
  (define runtime (assert-running-in-fiber-environment 'fiber-then))
  (define (transformer res)
    (match res
      [(success data) (immediate-fiber (data-transformer data))]
      [(failure err) (immediate-fiber (error-handler err))]))
  (make-chained-fiber #:runtime runtime
                      #:input-fiber fiber
                      #:transformer transformer
                      #:result-box (box #f)))

(define (fiber-then-chain fiber
                          data-transformer
                          [error-handler immediate-failed-fiber])
  (define runtime (assert-running-in-fiber-environment 'fiber-then-chain))
  (define (transformer res)
    (match res
      [(success data) (data-transformer data)]
      [(failure err) (error-handler err)]))
  (make-chained-fiber #:runtime runtime
                      #:input-fiber fiber
                      #:transformer transformer
                      #:result-box (box #f)))

(define (fiber-then-call fiber data-thunk)
  (define runtime (assert-running-in-fiber-environment 'fiber-then-call))
  (make-chained-fiber #:runtime runtime
                      #:input-fiber fiber
                      #:transformer (λ (_) (immediate-fiber (data-thunk)))
                      #:result-box (box #f)))

(define (fiber-then-call-chaining fiber data-thunk)
  (define runtime
    (assert-running-in-fiber-environment 'fiber-then-call-chaining))
  (make-chained-fiber #:runtime runtime
                      #:input-fiber fiber
                      #:transformer (λ (_) (data-thunk))
                      #:result-box (box #f)))

(define (fiber-combine data-combiner . fibers)
  (define runtime (assert-running-in-fiber-environment 'fiber-combine))
  (define (chaining-combiner . inputs)
    (immediate-fiber (apply data-combiner inputs)))
  (fiber-combine-chaining-internal chaining-combiner fibers #:runtime runtime))

(define (fiber-combine-chaining data-combiner . fibers)
  (define runtime (assert-running-in-fiber-environment 'fiber-combine-chaining))
  (fiber-combine-chaining-internal data-combiner fibers #:runtime runtime))

(define (fiber-combine-chaining-internal data-combiner fibers #:runtime runtime)
  (define (poller results)
    (or (transduce results #:into (into-any-match? present-failure?))
        (transduce results #:into (into-all-match? present?))))
  (define (combiner results)
    (define failure-opt
      (transduce results
                 (append-mapping in-option)
                 (filtering failure?)
                 (mapping failure-error)
                 #:into into-first))
    (match failure-opt
      [(present err) (immediate-failed-fiber err)]
      [absent
       (define inputs
         (transduce results
                    (mapping present-value)
                    (mapping success-value)
                    #:into into-list))
       (apply data-combiner inputs)]))
  (make-chained-fiber #:runtime runtime
                      #:input-fibers fibers
                      #:readiness-poller poller
                      #:combiner combiner
                      #:result-box (box #f)))

(define (present-failure? v)
  (and (present? v) (failure? (present-value v))))

(define (fiber-result fiber) (fiber-then fiber success failure))

(define (fiber-run-in-background fiber)
  (error 'fiber-run-in-background "not implemented"))

(define (void-fiber) (immediate-fiber (void)))

(define (sync-fiber evt)
  (define runtime (assert-running-in-fiber-environment 'sync-fiber))
  (make-external-fiber #:runtime runtime
                       #:event evt
                       #:result-box (box #f)))

;@------------------------------------------------------------------------------
;; Fiber engines

(define-object-type fiber-engine (scheduler))

(define standard-fiber-engine
  (make-fiber-engine
   #:name 'standard-fiber-engine
   #:scheduler
   (λ (fiber-maker) (error 'standard-fiber-engine "not implemented"))))

(define (fiber-engine-run engine fiber-maker)
  (sync-fiber (fiber-engine-run-evt engine fiber-maker)))

(define (fiber-engine-run-evt engine fiber-maker)
  ((fiber-engine-scheduler engine) fiber-maker))
