/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ql.expression.predicate.regex;

import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.RegExp;

public class RLikePattern implements StringPattern {

    private final String regexpPattern;

    public RLikePattern(String regexpPattern) {
        this.regexpPattern = regexpPattern;
    }

    @Override
    public String asJavaRegex() {
        return regexpPattern;
    }

    @Override
    public boolean matchesAll() {
        return Operations.isTotal(new RegExp(regexpPattern).toAutomaton());
    }
}
