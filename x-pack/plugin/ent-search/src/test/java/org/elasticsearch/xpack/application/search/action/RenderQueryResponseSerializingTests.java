/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search.action;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractNamedWriteableTestCase;

import java.io.IOException;
import java.util.Arrays;

public class RenderQueryResponseSerializingTests extends AbstractNamedWriteableTestCase<RenderSearchApplicationQueryAction.Response> {

    @Override
    protected RenderSearchApplicationQueryAction.Response createTestInstance() {
        return new RenderSearchApplicationQueryAction.Response(randomAlphaOfLengthBetween(5, 15), randomSearchSourceBuilder());
    }

    @Override
    protected RenderSearchApplicationQueryAction.Response mutateInstance(RenderSearchApplicationQueryAction.Response instance)
        throws IOException {
        return randomValueOtherThan(instance, this::createTestInstance);
    }

    protected SearchSourceBuilder randomSearchSourceBuilder() {
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        if (randomBoolean()) {
            searchSourceBuilder.query(new TermQueryBuilder(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLengthBetween(1, 10)));
        }
        if (randomBoolean()) {
            searchSourceBuilder.aggregation(
                new TermsAggregationBuilder(randomAlphaOfLengthBetween(1, 10)).field(randomAlphaOfLengthBetween(1, 10))
                    .collectMode(randomFrom(Aggregator.SubAggCollectionMode.values()))
            );
        }
        return searchSourceBuilder;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(
            Arrays.asList(
                new NamedWriteableRegistry.Entry(
                    RenderSearchApplicationQueryAction.Response.class,
                    RenderSearchApplicationQueryAction.NAME,
                    RenderSearchApplicationQueryAction.Response::new
                ),
                new NamedWriteableRegistry.Entry(AggregationBuilder.class, TermsAggregationBuilder.NAME, TermsAggregationBuilder::new),
                new NamedWriteableRegistry.Entry(QueryBuilder.class, TermQueryBuilder.NAME, TermQueryBuilder::new)
            )
        );
    }

    @Override
    protected Class<RenderSearchApplicationQueryAction.Response> categoryClass() {
        return RenderSearchApplicationQueryAction.Response.class;
    }
}
