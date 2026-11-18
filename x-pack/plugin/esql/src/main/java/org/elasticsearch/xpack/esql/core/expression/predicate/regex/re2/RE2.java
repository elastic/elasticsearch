/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * NOTE: forked from RE2/J 1.8, see package-info.java, keeping only the parse flags.
 *
 * Copyright (c) 2020 The Go Authors. All rights reserved.
 *
 * Use of this source code is governed by a BSD-style
 * license that can be found in the LICENSE file.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2;

// ES: only the parse flags of RE2/J's RE2, which also holds its matching
final class RE2 {

  //// Parser flags.

  // Fold case during matching (case-insensitive).
  static final int FOLD_CASE = 0x01;

  // Treat pattern as a literal string instead of a regexp.
  static final int LITERAL = 0x02;

  // Allow character classes like [^a-z] and [[:space:]] to match newline.
  static final int CLASS_NL = 0x04;

  // Allow '.' to match newline.
  static final int DOT_NL = 0x08;

  // Treat ^ and $ as only matching at beginning and end of text, not
  // around embedded newlines.  (Perl's default).
  static final int ONE_LINE = 0x10;

  // Make repetition operators default to non-greedy.
  static final int NON_GREEDY = 0x20;

  // allow Perl extensions:
  //   non-capturing parens - (?: )
  //   non-greedy operators - *? +? ?? {}?
  //   flag edits - (?i) (?-i) (?i: )
  //     i - FoldCase
  //     m - !OneLine
  //     s - DotNL
  //     U - NonGreedy
  //   line ends: \A \z
  //   \Q and \E to disable/enable metacharacters
  //   (?P<name>expr) for named captures
  // \C (any byte) is not supported.
  static final int PERL_X = 0x40;

  // Allow \p{Han}, \P{Han} for Unicode group and negation.
  static final int UNICODE_GROUPS = 0x80;

  // Regexp END_TEXT was $, not \z.  Internal use only.
  static final int WAS_DOLLAR = 0x100;

  static final int MATCH_NL = CLASS_NL | DOT_NL;

  // As close to Perl as possible.
  static final int PERL = CLASS_NL | ONE_LINE | PERL_X | UNICODE_GROUPS;

  // POSIX syntax.
  static final int POSIX = 0;

  private RE2() {}
}
