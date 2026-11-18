/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * The forked files of this package are a work derived from RE2/J, itself derived from Russ Cox's RE2 in Go, whose
 * license http://golang.org/LICENSE is as follows:
 *
 * Copyright (c) 2009 The Go Authors. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *    * Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *
 *    * Redistributions in binary form must reproduce the above copyright
 *      notice, this list of conditions and the following disclaimer in
 *      the documentation and/or other materials provided with the
 *      distribution.
 *
 *    * Neither the name of Google Inc. nor the names of its contributors
 *      may be used to endorse or promote products derived from this
 *      software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

/**
 * RE2 regular expressions, the syntax of PromQL: as Lucene automata for label matchers, see {@link
 * org.elasticsearch.xpack.esql.core.expression.predicate.regex.Re2RegExp}, and as a matcher with capture groups
 * for {@code label_replace}, see {@link org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.Pattern}.
 * <p>
 * It is a fork of <a href="https://github.com/google/re2j/tree/re2j-1.8/java/com/google/re2j">RE2/J 1.8</a>, under
 * the license above, which {@code licenses/re2j-LICENSE.txt} ships. RE2/J keeps its parser package
 * private and only matches strings, while Lucene needs an automaton, so both build on the one parser here.
 * The fork changes RE2/J so that it parses as Go 1.26's {@code regexp/syntax} does, which Prometheus 3.13 is
 * built with; the changes are marked {@code ES:}.
 * <ul>
 *     <li>Unicode classes and case folding come from the JDK ({@code Re2UnicodeTables}), not RE2/J's tables of Unicode 6.0,
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
 * </ul>
 * The forked files keep RE2/J's formatting, so that they diff against it line by line; spotless leaves them out.
 */
package org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2;
