/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestIngestDocument;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MatchProcessorTests extends ESTestCase {

    public void testBasics() throws Exception {
        int maxMatches = randomIntBetween(1, 8);
        MockSearchFunction mockSearch = mockedSearchFunction(Map.of("globalRank", 451, "tldRank", 23, "tld", "co"));
        MatchProcessor processor = new MatchProcessor(
            "_tag",
            null,
            mockSearch,
            "_name",
            str("domain"),
            str("entry"),
            true,
            false,
            "domain",
            maxMatches
        );
        IngestDocument ingestDocument = new IngestDocument(
            "_index",
            "_id",
            1L,
            "_routing",
            VersionType.INTERNAL,
            new HashMap<>(Map.of("domain", "elastic.co"))
        );
        // Run
        IngestDocument[] holder = new IngestDocument[1];
        processor.execute(ingestDocument, (result, e) -> holder[0] = result);
        assertThat(holder[0], notNullValue());
        // Check request
        SearchRequest request = mockSearch.getCapturedRequest();
        assertThat(request.indices().length, equalTo(1));
        assertThat(request.indices()[0], equalTo(".enrich-_name"));
        assertThat(request.preference(), equalTo(Preference.LOCAL.type()));
        assertThat(request.source().size(), equalTo(maxMatches));
        assertThat(request.source().trackScores(), equalTo(false));
        assertThat(request.source().fetchSource().fetchSource(), equalTo(true));
        assertThat(request.source().fetchSource().excludes(), emptyArray());
        assertThat(request.source().fetchSource().includes(), emptyArray());
        assertThat(request.source().query(), instanceOf(ConstantScoreQueryBuilder.class));
        assertThat(((ConstantScoreQueryBuilder) request.source().query()).innerQuery(), instanceOf(TermQueryBuilder.class));
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) ((ConstantScoreQueryBuilder) request.source().query()).innerQuery();
        assertThat(termQueryBuilder.fieldName(), equalTo("domain"));
        assertThat(termQueryBuilder.value(), equalTo("elastic.co"));
        // Check result
        Map<?, ?> entry;
        if (maxMatches == 1) {
            entry = ingestDocument.getFieldValue("entry", Map.class);
        } else {
            List<?> entries = ingestDocument.getFieldValue("entry", List.class);
            entry = (Map<?, ?>) entries.get(0);
        }
        assertThat(entry.size(), equalTo(3));
        assertThat(entry.get("globalRank"), equalTo(451));
        assertThat(entry.get("tldRank"), equalTo(23));
        assertThat(entry.get("tld"), equalTo("co"));
    }

    public void testNoMatch() throws Exception {
        MockSearchFunction mockSearch = mockedSearchFunction();
        MatchProcessor processor = new MatchProcessor(
            "_tag",
            null,
            mockSearch,
            "_name",
            str("domain"),
            str("entry"),
            true,
            false,
            "domain",
            1
        );
        IngestDocument ingestDocument = new IngestDocument(
            "_index",
            "_id",
            1L,
            "_routing",
            VersionType.INTERNAL,
            Map.of("domain", "elastic.com")
        );
        int numProperties = ingestDocument.getSourceAndMetadata().size();
        // Run
        IngestDocument[] holder = new IngestDocument[1];
        processor.execute(ingestDocument, (result, e) -> holder[0] = result);
        assertThat(holder[0], notNullValue());
        // Check request
        SearchRequest request = mockSearch.getCapturedRequest();
        assertThat(request.indices().length, equalTo(1));
        assertThat(request.indices()[0], equalTo(".enrich-_name"));
        assertThat(request.preference(), equalTo(Preference.LOCAL.type()));
        assertThat(request.source().size(), equalTo(1));
        assertThat(request.source().trackScores(), equalTo(false));
        assertThat(request.source().fetchSource().fetchSource(), equalTo(true));
        assertThat(request.source().fetchSource().includes(), emptyArray());
        assertThat(request.source().fetchSource().excludes(), emptyArray());
        assertThat(request.source().query(), instanceOf(ConstantScoreQueryBuilder.class));
        assertThat(((ConstantScoreQueryBuilder) request.source().query()).innerQuery(), instanceOf(TermQueryBuilder.class));
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) ((ConstantScoreQueryBuilder) request.source().query()).innerQuery();
        assertThat(termQueryBuilder.fieldName(), equalTo("domain"));
        assertThat(termQueryBuilder.value(), equalTo("elastic.com"));
        // Check result
        assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(numProperties));
    }

    public void testSearchFailure() throws Exception {
        String indexName = ".enrich-_name";
        MockSearchFunction mockSearch = mockedSearchFunction(new IndexNotFoundException(indexName));
        MatchProcessor processor = new MatchProcessor(
            "_tag",
            null,
            mockSearch,
            "_name",
            str("domain"),
            str("entry"),
            true,
            false,
            "domain",
            1
        );
        IngestDocument ingestDocument = new IngestDocument(
            "_index",
            "_id",
            1L,
            "_routing",
            VersionType.INTERNAL,
            new HashMap<>(Map.of("domain", "elastic.com"))
        );
        // Run
        IngestDocument[] resultHolder = new IngestDocument[1];
        Exception[] exceptionHolder = new Exception[1];
        processor.execute(ingestDocument, (result, e) -> {
            resultHolder[0] = result;
            exceptionHolder[0] = e;
        });
        assertThat(resultHolder[0], nullValue());
        assertThat(exceptionHolder[0], notNullValue());
        assertThat(exceptionHolder[0], instanceOf(IndexNotFoundException.class));
        // Check request
        SearchRequest request = mockSearch.getCapturedRequest();
        assertThat(request.indices().length, equalTo(1));
        assertThat(request.indices()[0], equalTo(".enrich-_name"));
        assertThat(request.preference(), equalTo(Preference.LOCAL.type()));
        assertThat(request.source().size(), equalTo(1));
        assertThat(request.source().trackScores(), equalTo(false));
        assertThat(request.source().fetchSource().fetchSource(), equalTo(true));
        assertThat(request.source().fetchSource().includes(), emptyArray());
        assertThat(request.source().fetchSource().excludes(), emptyArray());
        assertThat(request.source().query(), instanceOf(ConstantScoreQueryBuilder.class));
        assertThat(((ConstantScoreQueryBuilder) request.source().query()).innerQuery(), instanceOf(TermQueryBuilder.class));
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) ((ConstantScoreQueryBuilder) request.source().query()).innerQuery();
        assertThat(termQueryBuilder.fieldName(), equalTo("domain"));
        assertThat(termQueryBuilder.value(), equalTo("elastic.com"));
        // Check result
        assertThat(exceptionHolder[0].getMessage(), equalTo("no such index [" + indexName + "]"));
    }

    public void testIgnoreKeyMissing() throws Exception {
        {
            MatchProcessor processor = new MatchProcessor(
                "_tag",
                null,
                mockedSearchFunction(),
                "_name",
                str("domain"),
                str("entry"),
                true,
                true,
                "domain",
                1
            );
            IngestDocument ingestDocument = new IngestDocument("_index", "_id", 1L, "_routing", VersionType.INTERNAL, Map.of());

            assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(5));
            IngestDocument[] holder = new IngestDocument[1];
            processor.execute(ingestDocument, (result, e) -> holder[0] = result);
            assertThat(holder[0], notNullValue());
            assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(5));
        }
        {
            MatchProcessor processor = new MatchProcessor(
                "_tag",
                null,
                mockedSearchFunction(),
                "_name",
                str("domain"),
                str("entry"),
                true,
                false,
                "domain",
                1
            );
            IngestDocument ingestDocument = new IngestDocument("_index", "_id", 1L, "_routing", VersionType.INTERNAL, Map.of());
            IngestDocument[] resultHolder = new IngestDocument[1];
            Exception[] exceptionHolder = new Exception[1];
            processor.execute(ingestDocument, (result, e) -> {
                resultHolder[0] = result;
                exceptionHolder[0] = e;
            });
            assertThat(resultHolder[0], nullValue());
            assertThat(exceptionHolder[0], notNullValue());
            assertThat(exceptionHolder[0], instanceOf(IllegalArgumentException.class));
        }
    }

    public void testExistingFieldWithOverrideDisabled() throws Exception {
        MockSearchFunction mockSearch = mockedSearchFunction(Map.of("globalRank", 451, "tldRank", 23, "tld", "co"));
        MatchProcessor processor = new MatchProcessor(
            "_tag",
            null,
            mockSearch,
            "_name",
            str("domain"),
            str("entry"),
            false,
            false,
            "domain",
            1
        );

        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(new HashMap<>(Map.of("domain", "elastic.co", "tld", "tld")));
        IngestDocument[] resultHolder = new IngestDocument[1];
        Exception[] exceptionHolder = new Exception[1];
        processor.execute(ingestDocument, (result, e) -> {
            resultHolder[0] = result;
            exceptionHolder[0] = e;
        });
        assertThat(exceptionHolder[0], nullValue());
        assertThat(resultHolder[0].hasField("tld"), equalTo(true));
        assertThat(resultHolder[0].getFieldValue("tld", Object.class), equalTo("tld"));
    }

    public void testExistingNullFieldWithOverrideDisabled() throws Exception {
        MockSearchFunction mockSearch = mockedSearchFunction(Map.of("globalRank", 451, "tldRank", 23, "tld", "co"));
        MatchProcessor processor = new MatchProcessor(
            "_tag",
            null,
            mockSearch,
            "_name",
            str("domain"),
            str("entry"),
            false,
            false,
            "domain",
            1
        );

        Map<String, Object> source = new HashMap<>();
        source.put("domain", "elastic.co");
        source.put("tld", null);
        IngestDocument ingestDocument = TestIngestDocument.withDefaultVersion(source);
        IngestDocument[] resultHolder = new IngestDocument[1];
        Exception[] exceptionHolder = new Exception[1];
        processor.execute(ingestDocument, (result, e) -> {
            resultHolder[0] = result;
            exceptionHolder[0] = e;
        });
        assertThat(exceptionHolder[0], nullValue());
        assertThat(resultHolder[0].hasField("tld"), equalTo(true));
        assertThat(resultHolder[0].getFieldValue("tld", Object.class), equalTo(null));
    }

    public void testNumericValue() {
        MockSearchFunction mockSearch = mockedSearchFunction(Map.of("globalRank", 451, "tldRank", 23, "tld", "co"));
        MatchProcessor processor = new MatchProcessor(
            "_tag",
            null,
            mockSearch,
            "_name",
            str("domain"),
            str("entry"),
            false,
            true,
            "domain",
            1
        );
        IngestDocument ingestDocument = new IngestDocument(
            "_index",
            "_id",
            1L,
            "_routing",
            VersionType.INTERNAL,
            new HashMap<>(Map.of("domain", 2))
        );

        // Execute
        IngestDocument[] holder = new IngestDocument[1];
        processor.execute(ingestDocument, (result, e) -> holder[0] = result);
        assertThat(holder[0], notNullValue());

        // Check request
        SearchRequest request = mockSearch.getCapturedRequest();
        assertThat(request.source().query(), instanceOf(ConstantScoreQueryBuilder.class));
        assertThat(((ConstantScoreQueryBuilder) request.source().query()).innerQuery(), instanceOf(TermQueryBuilder.class));
        TermQueryBuilder termQueryBuilder = (TermQueryBuilder) ((ConstantScoreQueryBuilder) request.source().query()).innerQuery();
        assertThat(termQueryBuilder.fieldName(), equalTo("domain"));
        assertThat(termQueryBuilder.value(), equalTo(2));

        // Check result
        Map<?, ?> entry = ingestDocument.getFieldValue("entry", Map.class);
        assertThat(entry.size(), equalTo(3));
        assertThat(entry.get("globalRank"), equalTo(451));
        assertThat(entry.get("tldRank"), equalTo(23));
        assertThat(entry.get("tld"), equalTo("co"));
    }

    public void testArray() {
        MockSearchFunction mockSearch = mockedSearchFunction(Map.of("globalRank", 451, "tldRank", 23, "tld", "co"));
        MatchProcessor processor = new MatchProcessor(
            "_tag",
            null,
            mockSearch,
            "_name",
            str("domain"),
            str("entry"),
            false,
            true,
            "domain",
            1
        );
        IngestDocument ingestDocument = new IngestDocument(
            "_index",
            "_id",
            1L,
            "_routing",
            VersionType.INTERNAL,
            new HashMap<>(Map.of("domain", List.of("1", "2")))
        );

        // Execute
        IngestDocument[] holder = new IngestDocument[1];
        processor.execute(ingestDocument, (result, e) -> holder[0] = result);
        assertThat(holder[0], notNullValue());

        // Check request
        SearchRequest request = mockSearch.getCapturedRequest();
        assertThat(request.source().query(), instanceOf(ConstantScoreQueryBuilder.class));
        assertThat(((ConstantScoreQueryBuilder) request.source().query()).innerQuery(), instanceOf(TermsQueryBuilder.class));
        TermsQueryBuilder termQueryBuilder = (TermsQueryBuilder) ((ConstantScoreQueryBuilder) request.source().query()).innerQuery();
        assertThat(termQueryBuilder.fieldName(), equalTo("domain"));
        assertThat(termQueryBuilder.values().size(), equalTo(2));
        assertThat(termQueryBuilder.values().get(0), equalTo("1"));
        assertThat(termQueryBuilder.values().get(1), equalTo("2"));

        // Check result
        Map<?, ?> entry = ingestDocument.getFieldValue("entry", Map.class);
        assertThat(entry.size(), equalTo(3));
        assertThat(entry.get("globalRank"), equalTo(451));
        assertThat(entry.get("tldRank"), equalTo(23));
        assertThat(entry.get("tld"), equalTo("co"));
    }

    private static final class MockSearchFunction implements EnrichProcessorFactory.SearchRunner {
        private final List<Map<?, ?>> mockResponse;
        private final SetOnce<SearchRequest> capturedRequest;
        private final Exception exception;

        MockSearchFunction(Map<?, ?> mockResponse) {
            this.mockResponse = mockResponse.isEmpty() ? List.of() : List.of(mockResponse);
            this.exception = null;
            this.capturedRequest = new SetOnce<>();
        }

        MockSearchFunction(Exception exception) {
            this.mockResponse = null;
            this.exception = exception;
            this.capturedRequest = new SetOnce<>();
        }

        @Override
        public void accept(
            Object value,
            int maxMatches,
            Supplier<SearchRequest> searchRequestSupplier,
            BiConsumer<List<Map<?, ?>>, Exception> handler
        ) {
            capturedRequest.set(searchRequestSupplier.get());
            if (exception != null) {
                handler.accept(null, exception);
            } else {
                handler.accept(mockResponse, null);
            }
        }

        SearchRequest getCapturedRequest() {
            return capturedRequest.get();
        }
    }

    public MockSearchFunction mockedSearchFunction() {
        return new MockSearchFunction(Collections.emptyMap());
    }

    public MockSearchFunction mockedSearchFunction(Exception exception) {
        return new MockSearchFunction(exception);
    }

    public MockSearchFunction mockedSearchFunction(Map<?, ?> document) {
        return new MockSearchFunction(document);
    }

    static TemplateScript.Factory str(String stringLiteral) {
        return new TestTemplateService.MockTemplateScript.Factory(stringLiteral);
    }
}
