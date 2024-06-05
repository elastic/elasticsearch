/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import java.util.Random;

import static org.elasticsearch.test.AbstractBuilderTestCase.TEXT_ALIAS_FIELD_NAME;
import static org.elasticsearch.test.AbstractBuilderTestCase.TEXT_FIELD_NAME;
import static org.elasticsearch.test.ESTestCase.randomFrom;

/**
 * Utility class for creating random QueryBuilders.
 * So far only leaf queries like {@link MatchAllQueryBuilder}, {@link TermQueryBuilder} or
 * {@link IdsQueryBuilder} are returned.
 */
public class RandomQueryBuilder {

    /**
     * Create a new query of a random type
     * @param r random seed
     * @return a random {@link QueryBuilder}
     */
    public static QueryBuilder createQuery(Random r) {
        return switch (RandomNumbers.randomIntBetween(r, 0, 3)) {
            case 0 -> new MatchAllQueryBuilderTests().createTestQueryBuilder();
            case 1 -> new TermQueryBuilderTests().createTestQueryBuilder();
            case 2 ->
                // We make sure this query has no types to avoid deprecation warnings in the
                // tests that use this method.
                new IdsQueryBuilderTests().createTestQueryBuilder();
            case 3 -> createMultiTermQuery(r);
            default -> throw new UnsupportedOperationException();
        };
    }

    /**
     * Create a new multi term query of a random type
     * @param r random seed
     * @return a random {@link MultiTermQueryBuilder}
     */
    public static MultiTermQueryBuilder createMultiTermQuery(Random r) {
        // for now, only use String Rangequeries for MultiTerm test, numeric and date makes little sense
        // see issue #12123 for discussion
        MultiTermQueryBuilder multiTermQueryBuilder;
        String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME);
        switch (RandomNumbers.randomIntBetween(r, 0, 3)) {
            case 0 -> {
                RangeQueryBuilder stringRangeQuery = new RangeQueryBuilder(fieldName);
                stringRangeQuery.from("a" + RandomStrings.randomAsciiOfLengthBetween(r, 1, 10));
                stringRangeQuery.to("z" + RandomStrings.randomAsciiOfLengthBetween(r, 1, 10));
                multiTermQueryBuilder = stringRangeQuery;
            }
            case 1 -> multiTermQueryBuilder = new PrefixQueryBuilderTests().createTestQueryBuilder();
            case 2 -> multiTermQueryBuilder = new WildcardQueryBuilderTests().createTestQueryBuilder();
            case 3 -> multiTermQueryBuilder = new FuzzyQueryBuilder(fieldName, RandomStrings.randomAsciiOfLengthBetween(r, 1, 10));
            default -> throw new UnsupportedOperationException();
        }
        if (r.nextBoolean()) {
            multiTermQueryBuilder.boost(2.0f / RandomNumbers.randomIntBetween(r, 1, 20));
        }
        return multiTermQueryBuilder;
    }
}
