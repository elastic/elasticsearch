`re2-search.txt` and `re2-exhaustive.txt.bz2` are copied unchanged from Go 1.26.1, `src/regexp/testdata`, which
built them by running `make log` in the RE2 distribution (https://github.com/google/re2). Go's `regexp` package
tests itself with them in `src/regexp/exec_test.go`, and `Re2RegExpGoTestdataTests` reads them the same way.

They are under Go's BSD-style license, the license of the RE2/J sources the `re2` package forks, which is in
`x-pack/plugin/esql/licenses/re2j-LICENSE.txt`.
