/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.cluster.routing.Preference;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MatchProcessorTests extends ESTestCase {

    public void testBasics() throws Exception {
        int maxMatches = randomIntBetween(1, 8);
        MockSearchFunction mockSearch = mockedSearchFunction(mapOf("elastic.co", mapOf("globalRank", 451, "tldRank", 23, "tld", "co")));
        MatchProcessor processor = new MatchProcessor(
            "_tag",
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
            "_type",
            "_id",
            "_routing",
            1L,
            VersionType.INTERNAL,
            Collections.singletonMap("domain", "elastic.co")
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
        MatchProcessor processor = new MatchProcessor("_tag", mockSearch, "_name", str("domain"), str("entry"), true, false, "domain", 1);
        IngestDocument ingestDocument = new IngestDocument(
            "_index",
            "_type",
            "_id",
            "_routing",
            1L,
            VersionType.INTERNAL,
            Collections.singletonMap("domain", "elastic.com")
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
        MatchProcessor processor = new MatchProcessor("_tag", mockSearch, "_name", str("domain"), str("entry"), true, false, "domain", 1);
        IngestDocument ingestDocument = new IngestDocument(
            "_index",
            "_type",
            "_id",
            "_routing",
            1L,
            VersionType.INTERNAL,
            Collections.singletonMap("domain", "elastic.com")
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
                mockedSearchFunction(),
                "_name",
                str("domain"),
                str("entry"),
                true,
                true,
                "domain",
                1
            );
            IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL, mapOf());

            assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(6));
            IngestDocument[] holder = new IngestDocument[1];
            processor.execute(ingestDocument, (result, e) -> holder[0] = result);
            assertThat(holder[0], notNullValue());
            assertThat(ingestDocument.getSourceAndMetadata().size(), equalTo(6));
        }
        {
            MatchProcessor processor = new MatchProcessor(
                "_tag",
                mockedSearchFunction(),
                "_name",
                str("domain"),
                str("entry"),
                true,
                false,
                "domain",
                1
            );
            IngestDocument ingestDocument = new IngestDocument("_index", "_type", "_id", "_routing", 1L, VersionType.INTERNAL, mapOf());
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
        MockSearchFunction mockSearch = mockedSearchFunction(mapOf("elastic.co", mapOf("globalRank", 451, "tldRank", 23, "tld", "co")));
        MatchProcessor processor = new MatchProcessor("_tag", mockSearch, "_name", str("domain"), str("entry"), false, false, "domain", 1);

        IngestDocument ingestDocument = new IngestDocument(new HashMap<>(mapOf("domain", "elastic.co", "tld", "tld")), mapOf());
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
        MockSearchFunction mockSearch = mockedSearchFunction(mapOf("elastic.co", mapOf("globalRank", 451, "tldRank", 23, "tld", "co")));
        MatchProcessor processor = new MatchProcessor("_tag", mockSearch, "_name", str("domain"), str("entry"), false, false, "domain", 1);

        Map<String, Object> source = new HashMap<>();
        source.put("domain", "elastic.co");
        source.put("tld", null);
        IngestDocument ingestDocument = new IngestDocument(source, mapOf());
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
        MockSearchFunction mockSearch = mockedSearchFunction(mapOf(2, mapOf("globalRank", 451, "tldRank", 23, "tld", "co")));
        MatchProcessor processor = new MatchProcessor("_tag", mockSearch, "_name", str("domain"), str("entry"), false, true, "domain", 1);
        IngestDocument ingestDocument = new IngestDocument(
            "_index",
            "_type",
            "_id",
            "_routing",
            1L,
            VersionType.INTERNAL,
            mapOf("domain", 2)
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
        MockSearchFunction mockSearch = mockedSearchFunction(
            mapOf(Arrays.asList("1", "2"), mapOf("globalRank", 451, "tldRank", 23, "tld", "co"))
        );
        MatchProcessor processor = new MatchProcessor("_tag", mockSearch, "_name", str("domain"), str("entry"), false, true, "domain", 1);
        IngestDocument ingestDocument = new IngestDocument(
            "_index",
            "_type",
            "_id",
            "_routing",
            1L,
            VersionType.INTERNAL,
            mapOf("domain", Arrays.asList("1", "2"))
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

    private static final class MockSearchFunction implements BiConsumer<SearchRequest, BiConsumer<SearchResponse, Exception>> {
        private final SearchResponse mockResponse;
        private final SetOnce<SearchRequest> capturedRequest;
        private final Exception exception;

        MockSearchFunction(SearchResponse mockResponse) {
            this.mockResponse = mockResponse;
            this.exception = null;
            this.capturedRequest = new SetOnce<>();
        }

        MockSearchFunction(Exception exception) {
            this.mockResponse = null;
            this.exception = exception;
            this.capturedRequest = new SetOnce<>();
        }

        @Override
        public void accept(SearchRequest request, BiConsumer<SearchResponse, Exception> handler) {
            capturedRequest.set(request);
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
        return new MockSearchFunction(mockResponse(Collections.emptyMap()));
    }

    public MockSearchFunction mockedSearchFunction(Exception exception) {
        return new MockSearchFunction(exception);
    }

    public MockSearchFunction mockedSearchFunction(Map<?, Map<String, ?>> documents) {
        return new MockSearchFunction(mockResponse(documents));
    }

    public SearchResponse mockResponse(Map<?, Map<String, ?>> documents) {
        SearchHit[] searchHits = documents.entrySet().stream().map(e -> {
            SearchHit searchHit = new SearchHit(
                randomInt(100),
                e.getKey().toString(),
                new Text(MapperService.SINGLE_MAPPING_NAME),
                Collections.emptyMap()
            );
            try (XContentBuilder builder = XContentBuilder.builder(XContentType.SMILE.xContent())) {
                builder.map(e.getValue());
                builder.flush();
                ByteArrayOutputStream outputStream = (ByteArrayOutputStream) builder.getOutputStream();
                searchHit.sourceRef(new BytesArray(outputStream.toByteArray()));
            } catch (IOException ex) {
                throw new UncheckedIOException(ex);
            }
            return searchHit;
        }).toArray(SearchHit[]::new);
        return new SearchResponse(
            new SearchResponseSections(
                new SearchHits(searchHits, new TotalHits(documents.size(), TotalHits.Relation.EQUAL_TO), 1.0f),
                new Aggregations(Collections.emptyList()),
                new Suggest(Collections.emptyList()),
                false,
                false,
                null,
                1
            ),
            null,
            1,
            1,
            0,
            1,
            ShardSearchFailure.EMPTY_ARRAY,
            new SearchResponse.Clusters(1, 1, 0)
        );
    }

    static TemplateScript.Factory str(String stringLiteral) {
        return new TestTemplateService.MockTemplateScript.Factory(stringLiteral);
    }

    static <K, V> Map<K, V> mapOf() {
        return Collections.emptyMap();
    }

    static <K, V> Map<K, V> mapOf(K key1, V value1) {
        Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        return map;
    }

    static <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2) {
        Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        return map;
    }

    static Map<String, ?> mapOf(String key1, Object value1, String key2, Object value2, String key3, Object value3) {
        Map<String, Object> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        return map;
    }

    static <K, V> Map<K, V> mapOf(K key1, V value1, K key2, V value2, K key3, V value3, K key4, V value4) {
        Map<K, V> map = new HashMap<>();
        map.put(key1, value1);
        map.put(key2, value2);
        map.put(key3, value3);
        map.put(key4, value4);
        return map;
    }
}
