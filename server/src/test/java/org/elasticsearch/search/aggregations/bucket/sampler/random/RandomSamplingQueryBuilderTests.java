/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.sampler.random;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xcontent.XContentParseException;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class RandomSamplingQueryBuilderTests extends AbstractQueryTestCase<RandomSamplingQueryBuilder> {

    @Override
    protected RandomSamplingQueryBuilder doCreateTestQueryBuilder() {
        double probability = randomDoubleBetween(0.0, 1.0, false);
        var builder = new RandomSamplingQueryBuilder(probability);
        if (randomBoolean()) {
            builder.seed(randomInt());
        }
        if (randomBoolean()) {
            builder.hash(randomInt());
        }
        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(RandomSamplingQueryBuilder queryBuilder, Query query, SearchExecutionContext context)
        throws IOException {
        var rsQuery = asInstanceOf(RandomSamplingQuery.class, query);
        assertThat(rsQuery.probability(), equalTo(queryBuilder.probability()));
        assertThat(rsQuery.seed(), equalTo(queryBuilder.seed()));
        assertThat(rsQuery.hash(), equalTo(queryBuilder.hash()));
    }

    @Override
    protected boolean supportsBoost() {
        return false;
    }

    @Override
    protected boolean supportsQueryName() {
        return false;
    }

    @Override
    public void testUnknownField() {
        var json = "{ \""
            + RandomSamplingQueryBuilder.NAME
            + "\" : {\"bogusField\" : \"someValue\", \""
            + RandomSamplingQueryBuilder.PROBABILITY.getPreferredName()
            + "\" : \""
            + randomBoolean()
            + "\", \""
            + RandomSamplingQueryBuilder.SEED.getPreferredName()
            + "\" : \""
            + randomInt()
            + "\", \""
            + RandomSamplingQueryBuilder.HASH.getPreferredName()
            + "\" : \""
            + randomInt()
            + "\" } }";
        var e = expectThrows(XContentParseException.class, () -> parseQuery(json));
        assertTrue(e.getMessage().contains("bogusField"));
    }
}
