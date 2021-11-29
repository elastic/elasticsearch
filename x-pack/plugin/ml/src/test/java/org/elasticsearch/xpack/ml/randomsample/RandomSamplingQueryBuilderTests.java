/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.randomsample;

import org.apache.lucene.search.Query;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.AbstractQueryTestCase;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class RandomSamplingQueryBuilderTests extends AbstractQueryTestCase<RandomSamplingQueryBuilder> {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MachineLearning.class);
    }

    private boolean isCacheable = false;

    @Override
    protected RandomSamplingQueryBuilder doCreateTestQueryBuilder() {
        double p = randomDoubleBetween(0.00001, 0.999999, true);
        RandomSamplingQueryBuilder builder = new RandomSamplingQueryBuilder().setProbability(p);
        if (randomBoolean()) {
            builder.setSeed(123);
            isCacheable = true;
        }
        if (randomBoolean()) {
            builder.setQuery(QueryBuilders.matchAllQuery());
        }
        return builder;
    }

    @Override
    protected void doAssertLuceneQuery(RandomSamplingQueryBuilder queryBuilder, Query query, SearchExecutionContext context) {
        assertThat(query, instanceOf(RandomSamplingQuery.class));
    }

    public void testIllegalArguments() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new RandomSamplingQueryBuilder().setProbability(0.0)
        );
        assertEquals("[probability] cannot be less than or equal to 0.0.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RandomSamplingQueryBuilder().setProbability(-5.0));
        assertEquals("[probability] cannot be less than or equal to 0.0.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RandomSamplingQueryBuilder().setProbability(1.0));
        assertEquals("[probability] cannot be greater than or equal to 1.0.", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> new RandomSamplingQueryBuilder().setProbability(5.0));
        assertEquals("[probability] cannot be greater than or equal to 1.0.", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String json = "{\n" + "  \"random_sample\" : {\n" + "    \"probability\" : 0.5\n" + "  }\n" + "}";
        RandomSamplingQueryBuilder parsed = (RandomSamplingQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);
        assertThat(parsed.getProbability(), equalTo(0.5));
        assertThat(parsed.getSeed(), nullValue());

        // try with seed
        json = "{\n" + "  \"random_sample\" : {\n" + "    \"probability\" : 0.5,\n" + "    \"seed\" : 123\n" + "  }\n" + "}";
        parsed = (RandomSamplingQueryBuilder) parseQuery(json);
        assertThat(parsed.getProbability(), equalTo(0.5));
        assertThat(parsed.getSeed(), equalTo(123));
    }

}
