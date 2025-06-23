/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.mustache;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class SearchTemplateResponseTests extends AbstractXContentTestCase<SearchTemplateResponse> {

    @Override
    protected SearchTemplateResponse createTestInstance() {
        SearchTemplateResponse response = new SearchTemplateResponse();
        if (randomBoolean()) {
            response.setResponse(createSearchResponse());
        } else {
            response.setSource(createSource());
        }
        return response;
    }

    @Override
    protected SearchTemplateResponse doParseInstance(XContentParser parser) throws IOException {
        SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
        Map<String, Object> contentAsMap = parser.map();

        if (contentAsMap.containsKey(SearchTemplateResponse.TEMPLATE_OUTPUT_FIELD.getPreferredName())) {
            Object source = contentAsMap.get(SearchTemplateResponse.TEMPLATE_OUTPUT_FIELD.getPreferredName());
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).value(source);
            searchTemplateResponse.setSource(BytesReference.bytes(builder));
        } else {
            XContentType contentType = parser.contentType();
            XContentBuilder builder = XContentFactory.contentBuilder(contentType).map(contentAsMap);
            try (
                XContentParser searchResponseParser = XContentHelper.createParserNotCompressed(
                    XContentParserConfiguration.EMPTY.withRegistry(parser.getXContentRegistry())
                        .withDeprecationHandler(parser.getDeprecationHandler()),
                    BytesReference.bytes(builder),
                    contentType
                )
            ) {
                searchTemplateResponse.setResponse(SearchResponseUtils.parseSearchResponse(searchResponseParser));
            }
        }
        return searchTemplateResponse;
    }

    /**
     * For simplicity we create a minimal response, as there is already a dedicated
     * test class for search response parsing and serialization.
     */
    private static SearchResponse createSearchResponse() {
        long tookInMillis = randomNonNegativeLong();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = randomIntBetween(0, totalShards);

        return SearchResponseUtils.emptyWithTotalHits(
            null,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
    }

    private static BytesReference createSource() {
        try {
            XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("query")
                .startObject("match")
                .field(randomAlphaOfLength(5), randomAlphaOfLength(10))
                .endObject()
                .endObject()
                .endObject();
            return BytesReference.bytes(source);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        String templateOutputField = SearchTemplateResponse.TEMPLATE_OUTPUT_FIELD.getPreferredName();
        return field -> field.equals(templateOutputField) || field.startsWith(templateOutputField + ".");
    }

    /**
     * Note that we can't rely on normal equals and hashCode checks, since {@link SearchResponse} doesn't
     * currently implement equals and hashCode. Instead, we compare the template outputs for equality,
     * and perform some sanity checks on the search response instances.
     */
    @Override
    protected void assertEqualInstances(SearchTemplateResponse expectedInstance, SearchTemplateResponse newInstance) {
        assertNotSame(newInstance, expectedInstance);

        BytesReference expectedSource = expectedInstance.getSource();
        BytesReference newSource = newInstance.getSource();
        assertEquals(expectedSource == null, newSource == null);
        if (expectedSource != null) {
            try {
                assertToXContentEquivalent(expectedSource, newSource, XContentType.JSON);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        assertEquals(expectedInstance.hasResponse(), newInstance.hasResponse());
        if (expectedInstance.hasResponse()) {
            SearchResponse expectedResponse = expectedInstance.getResponse();
            SearchResponse newResponse = newInstance.getResponse();

            assertEquals(expectedResponse.getHits().getTotalHits().value(), newResponse.getHits().getTotalHits().value());
            assertEquals(expectedResponse.getHits().getMaxScore(), newResponse.getHits().getMaxScore(), 0.0001);
        }
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    public void testSourceToXContent() throws IOException {
        SearchTemplateResponse response = new SearchTemplateResponse();
        try {
            XContentBuilder source = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("query")
                .startObject("terms")
                .field("status", new String[] { "pending", "published" })
                .endObject()
                .endObject()
                .endObject();
            response.setSource(BytesReference.bytes(source));

            XContentType contentType = randomFrom(XContentType.values());
            XContentBuilder expectedResponse = XContentFactory.contentBuilder(contentType)
                .startObject()
                .startObject("template_output")
                .startObject("query")
                .startObject("terms")
                .field("status", new String[] { "pending", "published" })
                .endObject()
                .endObject()
                .endObject()
                .endObject();

            XContentBuilder actualResponse = XContentFactory.contentBuilder(contentType);
            response.toXContent(actualResponse, ToXContent.EMPTY_PARAMS);

            assertToXContentEquivalent(BytesReference.bytes(expectedResponse), BytesReference.bytes(actualResponse), contentType);
        } finally {
            response.decRef();
        }
    }

    public void testSearchResponseToXContent() throws IOException {
        SearchHit hit = SearchHit.unpooled(1, "id");
        hit.score(2.0f);
        SearchHit[] hits = new SearchHit[] { hit };

        SearchResponse searchResponse = new SearchResponse(
            SearchHits.unpooled(hits, new TotalHits(100, TotalHits.Relation.EQUAL_TO), 1.5f),
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

        SearchTemplateResponse response = new SearchTemplateResponse();
        try {
            response.setResponse(searchResponse);

            XContentType contentType = randomFrom(XContentType.values());
            XContentBuilder expectedResponse = XContentFactory.contentBuilder(contentType)
                .startObject()
                .field("took", 0)
                .field("timed_out", false)
                .startObject("_shards")
                .field("total", 0)
                .field("successful", 0)
                .field("skipped", 0)
                .field("failed", 0)
                .endObject()
                .startObject("hits")
                .startObject("total")
                .field("value", 100)
                .field("relation", "eq")
                .endObject()
                .field("max_score", 1.5F)
                .startArray("hits")
                .startObject()
                .field("_id", "id")
                .field("_score", 2.0F)
                .endObject()
                .endArray()
                .endObject()
                .endObject();

            XContentBuilder actualResponse = XContentFactory.contentBuilder(contentType);
            response.toXContent(actualResponse, ToXContent.EMPTY_PARAMS);

            assertToXContentEquivalent(BytesReference.bytes(expectedResponse), BytesReference.bytes(actualResponse), contentType);
        } finally {
            response.decRef();
        }
    }

    @Override
    protected void dispose(SearchTemplateResponse instance) {
        instance.decRef();
    }
}
