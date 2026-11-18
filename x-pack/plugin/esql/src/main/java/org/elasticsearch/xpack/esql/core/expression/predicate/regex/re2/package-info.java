/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/**
 * RE2 regular expressions, the syntax of PromQL label matchers, as Lucene automata; see {@link
 * org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.Re2RegExp}.
 * <p>
 * The parser is a fork of the one of
 * <a href="https://github.com/google/re2j/tree/re2j-1.8/java/com/google/re2j">RE2/J 1.8</a>, under its BSD license
 * (see {@code licenses/re2j-LICENSE.txt}): {@code Parser}, {@code Regexp}, {@code CharClass}, {@code CharGroup},
 * {@code Utils} and {@code PatternSyntaxException}, plus the parse flags of {@code RE2}. RE2/J keeps them package
 * private, and only matches strings, while Lucene needs an automaton. The fork changes RE2/J so that it parses as
 * Go 1.26's {@code regexp/syntax} does, which Prometheus 3.13 is built with; the changes are marked {@code ES:}.
 * <ul>
 *     <li>Unicode classes and case folding come from the JDK ({@code Unicode}), not RE2/J's tables of Unicode 6.0,
 *     and {@code \p} names are looked up as Go 1.26 does, with {@code Any}, {@code ASCII}, {@code Assigned},
 *     {@code Cn}, {@code LC}, the long category aliases, and case, space, underscore and hyphen ignored.</li>
 *     <li>A <code>&#123;</code> that starts no repeat count is a literal that a repetition may follow, as in <code>&#123;*</code>.</li>
 *     <li>Nested counted repeats are bounded to 1000 copies of their innermost expression.</li>
 *     <li>A capture group name may be used more than once.</li>
 *     <li>A {@code )} that closes no group is reported as unexpected, and a trailing backslash with the empty
 *     expression, as Go reports them.</li>
 *     <li>{@code (?<} or {@code (?P<} with nothing after it is not taken as a named group.</li>
 *     <li>A backslash before a non-ASCII character is an invalid escape.</li>
 *     <li>A case-insensitive literal is not equal to the case-sensitive one, so alternatives don't share it as a
 *     prefix.</li>
 *     <li>{@code PatternSyntaxException} is an {@link java.lang.IllegalArgumentException}.</li>
 *     <li>{@code RE2} keeps only the parse flags; RE2/J's own tables and {@code Unicode} are replaced, and the
 *     parse is otherwise unchanged.</li>
 * </ul>
 * The forked files keep RE2/J's formatting, so that they diff against it line by line; spotless leaves them out.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2;
