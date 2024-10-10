/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.fulltext;

import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.FullTextPredicate;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public abstract class AbstractFulltextSerializationTests<T extends FullTextPredicate> extends AbstractExpressionSerializationTests<T> {

    static final String OPTION_DELIMITER = ";";

    String randomOptionOrNull() {
        if (randomBoolean()) {
            return null;
        }
        HashMap<String, String> options = new HashMap<>();
        int maxOptions = randomInt(8);
        for (int i = 0; i < maxOptions; i++) {
            var opt = randomIndividualOption();
            options.computeIfAbsent(opt.getKey(), k -> opt.getValue()); // no duplicate options
        }
        return options.entrySet().stream().map(e -> e.getKey() + "=" + e.getValue()).collect(Collectors.joining(OPTION_DELIMITER));
    }

    Map.Entry<String, String> randomIndividualOption() {
        return Map.entry(randomAlphaOfLength(randomIntBetween(1, 4)), randomAlphaOfLength(randomIntBetween(1, 4)));
    }
}
