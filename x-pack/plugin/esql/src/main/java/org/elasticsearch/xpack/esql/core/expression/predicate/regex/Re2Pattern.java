/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.expression.predicate.regex;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.re2.RE2;

import java.util.Objects;

/**
 * A regular expression in RE2 syntax, the syntax of PromQL label matchers, matched against the whole string. Unlike
 * {@link RLikePattern}, which takes Lucene's regexp syntax; see {@link Re2RegExp}.
 */
public class Re2Pattern extends AbstractStringPattern {

    private final String regex;

    public Re2Pattern(String regex) {
        this.regex = regex;
    }

    @Override
    protected Automaton doCreateAutomaton(boolean ignoreCase) {
        int flags = RE2.PERL | RE2.DOT_NL;
        if (ignoreCase) {
            flags |= RE2.FOLD_CASE;
        }
        return Operations.determinize(new Re2RegExp(regex, flags).toAutomaton(), Operations.DEFAULT_DETERMINIZE_WORK_LIMIT);
    }

    /**
     * RE2 and Java differ in syntax and semantics, in {@code \p} names and case folding among others, so there is no
     * Java regex to give; {@code Re2Like} matches with the automaton.
     */
    @Override
    public String asJavaRegex() {
        throw new UnsupportedOperationException("an RE2 regular expression has no Java equivalent");
    }

    @Override
    public String pattern() {
        return regex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Re2Pattern that = (Re2Pattern) o;
        return Objects.equals(regex, that.regex);
    }

    @Override
    public int hashCode() {
        return Objects.hash(regex);
    }
}
