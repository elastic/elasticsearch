/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.regex;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.CharacterRunAutomaton;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;

public class RegexMatch {
    @Evaluator
    static boolean process(BytesRef input, @Fixed CharacterRunAutomaton pattern) {
        if (input == null) {
            return false;
        }
        return pattern.run(input.utf8ToString());
    }
}
