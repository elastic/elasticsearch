/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.RegExp;

public class RLikePattern extends AbstractStringPattern {

    private final String regexpPattern;

    public RLikePattern(String regexpPattern) {
        this.regexpPattern = regexpPattern;
    }

    @Override
    Automaton createAutomaton() {
        return new RegExp(regexpPattern).toAutomaton();
    }

    @Override
    public String asString() {
        return regexpPattern;
    }

    @Override
    public String asJavaRegex() {
        return regexpPattern;
    }
}
