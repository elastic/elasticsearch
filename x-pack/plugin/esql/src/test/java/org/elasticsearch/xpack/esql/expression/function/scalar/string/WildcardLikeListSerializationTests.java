/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.string;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPattern;
import org.elasticsearch.xpack.esql.core.expression.predicate.regex.WildcardPatternList;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.regex.WildcardLikeList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.abs;

public class WildcardLikeListSerializationTests extends AbstractExpressionSerializationTests<WildcardLikeList> {
    @Override
    protected WildcardLikeList createTestInstance() {
        Source source = randomSource();
        Expression child = randomChild();
        return new WildcardLikeList(source, child, generateRandomPatternList());
    }

    @Override
    protected WildcardLikeList mutateInstance(WildcardLikeList instance) throws IOException {
        Source source = instance.source();
        Expression child = instance.field();
        return new WildcardLikeList(source, child, generateRandomPatternList());
    }

    private WildcardPatternList generateRandomPatternList() {
        int numChildren = abs(randomInt() % 10) + 1; // Ensure at least one child
        List<WildcardPattern> patterns = new ArrayList<>(numChildren);
        for (int i = 0; i < numChildren; i++) {
            WildcardPattern pattern = new WildcardPattern(randomAlphaOfLength(4));
            patterns.add(pattern);
        }
        return new WildcardPatternList(patterns);
    }
}
