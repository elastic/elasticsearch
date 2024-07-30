/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.predicate.operator.fulltext;

import org.elasticsearch.xpack.esql.core.expression.predicate.fulltext.MultiMatchQueryPredicate;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MultiMatchQuerySerializationTests extends AbstractFulltextSerializationTests<MultiMatchQueryPredicate> {

    @Override
    protected final MultiMatchQueryPredicate createTestInstance() {
        return new MultiMatchQueryPredicate(
            randomSource(),
            randomFieldString(),
            randomAlphaOfLength(randomIntBetween(1, 16)),
            randomOptionOrNull()
        );
    }

    @Override
    protected MultiMatchQueryPredicate mutateInstance(MultiMatchQueryPredicate instance) throws IOException {
        var fieldString = instance.fieldString();
        var query = instance.query();
        var options = instance.options();
        switch (between(0, 2)) {
            case 0 -> fieldString = randomValueOtherThan(fieldString, this::randomFieldString);
            case 1 -> query = randomValueOtherThan(query, () -> randomAlphaOfLength(randomIntBetween(1, 16)));
            case 2 -> options = randomValueOtherThan(options, this::randomOptionOrNull);
        }
        return new MultiMatchQueryPredicate(instance.source(), fieldString, query, options);
    }

    String randomFieldString() {
        if (randomBoolean()) {
            return ""; // empty, no fields
        }
        HashMap<String, Float> fields = new HashMap<>();
        int maxOptions = randomInt(4);
        for (int i = 0; i < maxOptions; i++) {
            var opt = randomIndividualField();
            fields.computeIfAbsent(opt.getKey(), k -> opt.getValue()); // no duplicate fields
        }
        return fields.entrySet().stream().map(e -> e.getKey() + "^" + e.getValue()).collect(Collectors.joining(","));
    }

    Map.Entry<String, Float> randomIndividualField() {
        return Map.entry(randomAlphaOfLength(randomIntBetween(1, 4)), randomFloat());
    }
}
