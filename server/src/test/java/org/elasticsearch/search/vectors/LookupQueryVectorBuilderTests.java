/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.test.AbstractQueryVectorBuilderTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class LookupQueryVectorBuilderTests extends AbstractQueryVectorBuilderTestCase<LookupQueryVectorBuilder> {
    @Override
    protected void doAssertClientRequest(ActionRequest request, LookupQueryVectorBuilder builder) {
        assertThat(request, instanceOf(SearchRequest.class));
        SearchRequest searchRequest = (SearchRequest) request;
        assertThat(searchRequest.source().query(), instanceOf(IdsQueryBuilder.class));
        IdsQueryBuilder idsQuery = (IdsQueryBuilder) searchRequest.source().query();
        assertThat(idsQuery.ids(), hasSize(1));
        assertTrue(idsQuery.ids().contains(builder.getId()));
        assertThat(searchRequest.indices()[0], equalTo(builder.getIndex()));
        if (builder.getRouting() != null) {
            assertThat(searchRequest.routing(), equalTo(builder.getRouting()));
        } else {
            assertThat(searchRequest.routing(), equalTo(null));
        }
        assertThat(searchRequest.source().size(), equalTo(1));
        assertThat(searchRequest.source().fetchSource().fetchSource(), equalTo(false));
    }

    @Override
    protected ActionResponse createResponse(float[] array, LookupQueryVectorBuilder builder) {
        SearchHit hit = SearchHit.unpooled(1, builder.getId());
        hit.addDocumentFields(Map.of(builder.getPath(), new DocumentField(builder.getPath(), List.of(array))), Map.of());
        SearchHits sHits = SearchHits.unpooled(new SearchHit[] { hit }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        return new SearchResponse(
            sHits,
            null,
            null,
            false,
            null,
            null,
            1,
            null,
            0,
            0,
            0,
            0,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    @Override
    protected LookupQueryVectorBuilder doParseInstance(XContentParser parser) throws IOException {
        return LookupQueryVectorBuilder.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<LookupQueryVectorBuilder> instanceReader() {
        return LookupQueryVectorBuilder::new;
    }

    @Override
    protected LookupQueryVectorBuilder createTestInstance() {
        return new LookupQueryVectorBuilder(
            randomAlphaOfLengthBetween(1, 20),
            randomAlphaOfLengthBetween(1, 20),
            randomAlphaOfLengthBetween(1, 20),
            randomBoolean() ? randomAlphaOfLengthBetween(1, 20) : null
        );
    }

    @Override
    protected LookupQueryVectorBuilder mutateInstance(LookupQueryVectorBuilder instance) throws IOException {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new LookupQueryVectorBuilder(
                instance.getId() + "mutated",
                instance.getIndex(),
                instance.getPath(),
                instance.getRouting()
            );
            case 1 -> new LookupQueryVectorBuilder(
                instance.getId(),
                instance.getIndex() + "mutated",
                instance.getPath(),
                instance.getRouting()
            );
            case 2 -> new LookupQueryVectorBuilder(
                instance.getId(),
                instance.getIndex(),
                instance.getPath() + "mutated",
                instance.getRouting()
            );
            case 3 -> new LookupQueryVectorBuilder(
                instance.getId(),
                instance.getIndex(),
                instance.getPath(),
                instance.getRouting() == null ? "mutatedRouting" : instance.getRouting() + "mutated"
            );
            default -> throw new IllegalStateException("Unexpected value");
        };
    }
}
