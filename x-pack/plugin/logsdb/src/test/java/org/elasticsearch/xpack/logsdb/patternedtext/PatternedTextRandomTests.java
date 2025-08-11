/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.datageneration.queries.LeafQueryGenerator;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.logsdb.LogsDBPlugin;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;

public class PatternedTextRandomTests extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal, otherSettings))
//                .put("xpack.license.self_generated.type", "trial")
//                .put("cluster.logsdb.enabled", "true")
                .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
                .build();
    }



    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
//        return Arrays.asList(LocalStateCompositeXPackPlugin.class);
        return Arrays.asList(MapperExtrasPlugin.class, LogsDBPlugin.class, LocalStateCompositeXPackPlugin.class);
//        return CollectionUtils.appendToCopy(super.nodePlugins(), LogsDBPlugin.class, LocalStateCompositeXPackPlugin.class);
//        return List.of(InternalSettingsPlugin.class, XPackPlugin.class, LogsDBPlugin.class, DataStreamsPlugin.class);
    }

    private static final String INDEX = "test_index";
    private static final String MATCH_ONLY_TEXT_FIELD = "field_match_only_text";
    private static final String PATTERNED_TEXT_FIELD = "field_patterned_text";

    @Before
    public void setup() {
        assumeTrue("Only when patterned_text feature flag is enabled", PatternedTextFieldMapper.PATTERNED_TEXT_MAPPER.isEnabled());
    }

    public void test() throws IOException {
        var settings = Settings.builder();
//        var settings = Settings.builder().put(IndexSettings.MODE.getKey(), IndexMode.LOGSDB.getName());
        var mappings = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("@timestamp")
            .field("type", "date")
            .endObject()
            .startObject(PATTERNED_TEXT_FIELD)
            .field("type", "patterned_text")
            .endObject()
            .startObject(MATCH_ONLY_TEXT_FIELD)
            .field("type", "match_only_text")
            .endObject()
            .endObject()
            .endObject();

        var createRequest = new CreateIndexRequest(INDEX)
            .settings(settings)
            .mapping(mappings);

        var createResponse = safeGet(admin().indices().create(createRequest));
        assertTrue(createResponse.isAcknowledged());

        int numDocs = randomIntBetween(1, 300);
        List<String> logMessages = generateMessages(numDocs);
        indexDocs(logMessages);

        for (var message : logMessages) {
            List<String> queryTerms = randomQueryParts(message);

            var patternedTextQueries = generateQueries(PATTERNED_TEXT_FIELD, queryTerms);
            var matchOnlyQueries = generateQueries(MATCH_ONLY_TEXT_FIELD, queryTerms);

            for (int i = 0; i < patternedTextQueries.size(); ++i) {
                var ptQuery = patternedTextQueries.get(i);
                var motQuery = matchOnlyQueries.get(i);

                var ptResponse = client().prepareSearch(INDEX).setQuery(ptQuery).setSize(numDocs).get();
                var motResponse = client().prepareSearch(INDEX).setQuery(motQuery).setSize(numDocs).get();

                assertNoFailures(ptResponse);
                assertNoFailures(motResponse);

//                assertTrue(motResponse.getHits().getTotalHits().value() > 0);
                assertEquals(
                    motResponse.getHits().getTotalHits().value(),
                    ptResponse.getHits().getTotalHits().value()
                );

                var motDocIds = Arrays.stream(motResponse.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toSet());
                var ptDocIds = Arrays.stream(ptResponse.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toSet());

                assertEquals(motDocIds, ptDocIds);
            }
        }
    }

    public List<QueryBuilder> generateQueries(String field, List<String> queryTerms) {
        var results = new ArrayList<QueryBuilder>();

        for (var queryTerm : queryTerms) {
            results.add(QueryBuilders.termQuery(field, queryTerm));
            results.add(QueryBuilders.matchQuery(field, queryTerm));
            results.add(QueryBuilders.matchPhraseQuery(field, queryTerm));
        }

        return results;
    }

    private static List<String> randomQueryParts(String value) {
        var values = new ArrayList<String>();
        var tokenizerRegex = "[\\s\\p{Punct}]+";
        List<String> tokens = Arrays.stream(value.split(tokenizerRegex)).filter(t -> t.isEmpty() == false).toList();

        // full value
        values.add(value);
        // random sub-phrase
        values.add(randomSubstring(value));

        if (tokens.isEmpty() == false) {
            // random term
            values.add(randomFrom(tokens));
            // random sub-phrase
            values.add(getSubPhrase(tokens));
        }
        return values;
    }

    private static String randomSubstring(String value) {
        int low = ESTestCase.randomIntBetween(0, value.length() - 1);
        int hi = ESTestCase.randomIntBetween(low + 1, value.length());
        return value.substring(low, hi);
    }

    private static String getSubPhrase(List<String> tokens) {
        int low = ESTestCase.randomIntBetween(0, tokens.size() - 1);
        int hi = ESTestCase.randomIntBetween(low + 1, tokens.size());
        return String.join(" ", tokens.subList(low, hi));
    }

    private List<String> generateMessages(int numDocs) {
        List<String> logMessages = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            logMessages.add(randomMessage());
        }
        return logMessages;
    }

    private void indexDocs(List<String> logMessages) throws IOException {
        BulkRequest bulkRequest = new BulkRequest();
        long timestamp = System.currentTimeMillis();
        for (var msg : logMessages) {
            timestamp += TimeUnit.SECONDS.toMillis(1);
            var indexRequest = new IndexRequest(INDEX).opType(DocWriteRequest.OpType.CREATE)
                .source(
                    JsonXContent.contentBuilder()
                        .startObject()
                        .field("@timestamp", timestamp)
                        .field("field_patterned_text", msg)
                        .field("field_match_only_text", msg)
                        .endObject()
                );
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures());
        safeGet(indicesAdmin().refresh(new RefreshRequest(INDEX).indicesOptions(IndicesOptions.lenientExpandOpenHidden())));
    }

    public static String randomMessage() {
        if (rarely()) {
            return randomRealisticUnicodeOfCodepointLength(randomIntBetween(1, 100));
        }

        StringBuilder message = new StringBuilder();
        int numTokens = randomIntBetween(1, 30);

        if (randomBoolean()) {
            message.append("[").append(randomTimestamp()).append("]");
        }
        for (int i = 0; i < numTokens; i++) {
            message.append(randomSeparator());

            if (randomBoolean()) {
                message.append(randomSentence());
            } else {
                var token = randomFrom(
                    random(),
                    () -> randomRealisticUnicodeOfCodepointLength(randomIntBetween(1, 20)),
                    () -> UUID.randomUUID().toString(),
                    () -> randomIp(randomBoolean()),
                    PatternedTextRandomTests::randomTimestamp,
                    ESTestCase::randomInt,
                    ESTestCase::randomDouble
                );

                if (randomBoolean()) {
                    message.append("[").append(token).append("]");
                } else {
                    message.append(token);
                }
            }
        }
        return message.toString();
    }

    private static StringBuilder randomSentence() {
        int words = randomIntBetween(1, 10);
        StringBuilder text = new StringBuilder();
        for (int i = 0; i < words; i++) {
            if (i > 0) {
                text.append(" ");
            }
            text.append(randomAlphaOfLength(randomIntBetween(1, 10)));
        }
        return text;
    }

    private static String randomSeparator() {
        if (randomBoolean()) {
            // Return spaces frequently since current token splitting is on spaces.
            return " ".repeat(randomIntBetween(1, 10));
        } else {
            return randomFrom("\t\n;:.',".split(""));
        }
    }

    private static String randomTimestamp() {
        long millis = randomMillisUpToYear9999();
        ZonedDateTime zonedDateTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(millis), randomZone());
        DateFormatter formatter = DateFormatter.forPattern(randomDateFormatterPattern()).withLocale(randomLocale(random()));
        return formatter.format(zonedDateTime);
    }
}
