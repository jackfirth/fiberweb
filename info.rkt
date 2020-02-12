#lang info

(define collection "fiberweb")

(define scribblings
  (list (list "main.scrbl"
              (list 'multi-page)
              (list 'library)
              "fiberweb")))

(define deps
  (list "base"))

(define build-deps
  (list "racket-doc"
        "rackunit-lib"
        "scribble-lib"))
