/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patterntext;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.logsdb.LogsDBPlugin;
import org.junit.After;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;

public class PatternTextIntegrationTests extends ESSingleNodeTestCase {
    private static final Logger logger = LogManager.getLogger(PatternTextIntegrationTests.class);

    @ParametersFactory(argumentFormatting = "indexOptions=%s, disableTemplating=%b")
    public static List<Object[]> args() {
        List<Object[]> args = new ArrayList<>();
        for (var indexOption : new String[] { "docs", "positions" }) {
            for (var templating : new boolean[] { true, false }) {
                args.add(new Object[] { indexOption, templating });
            }
        }
        return Collections.unmodifiableList(args);
    }

    private final String indexOptions;
    private final boolean disableTemplating;
    private final String mapping;

    public PatternTextIntegrationTests(String indexOptions, boolean disableTemplating) {
        this.indexOptions = indexOptions;
        this.disableTemplating = disableTemplating;
        this.mapping = getMapping(indexOptions, disableTemplating);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial").build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MapperExtrasPlugin.class, XPackPlugin.class, LogsDBPlugin.class);
    }

    private static final String INDEX = "test_index";
    private static final String MATCH_ONLY_TEXT_FIELD = "field_match_only_text";
    private static final String PATTERN_TEXT_FIELD = "field_pattern_text";
    private static final String MAPPING_TEMPLATE = """
            {
              "properties": {
                "@timestamp": { "type": "date" },
                "field_match_only_text": { "type": "match_only_text" },
                "field_pattern_text": {
                  "type": "pattern_text",
                  "index_options": "%index_options%",
                  "disable_templating": "%disable_templating%",
                  "analyzer": "standard"
                }
              }
            }
        """;

    private static final Settings LOGSDB_SETTING = Settings.builder().put(IndexSettings.MODE.getKey(), "logsdb").build();

    @After
    public void cleanup() {
        assertAcked(admin().indices().prepareDelete(INDEX));
    }

    private String getMapping(String indexOptions, boolean disableTemplating) {
        return MAPPING_TEMPLATE.replace("%index_options%", indexOptions)
            .replace("%disable_templating%", Boolean.toString(disableTemplating));
    }

    public void testSourceMatchAllManyValues() throws IOException {
        var createRequest = indicesAdmin().prepareCreate(INDEX).setSettings(LOGSDB_SETTING).setMapping(mapping);
        createIndex(INDEX, createRequest);

        int numDocs = randomIntBetween(1, 100);
        List<String> messages = randomMessagesOfVariousSizes(numDocs);
        indexDocs(messages);

        assertMappings();
        assertMessagesInSource(messages);
    }

    public void testLargeValueIsStored() throws IOException {
        var createRequest = indicesAdmin().prepareCreate(INDEX).setSettings(LOGSDB_SETTING).setMapping(mapping);
        IndexService indexService = createIndex(INDEX, createRequest);

        // large message
        String message = randomMessage(randomIntBetween(8 * 1024, 100_000));
        List<String> messages = List.of(message);
        indexDocs(messages);

        assertMappings();
        assertMessagesInSource(messages);

        // assert contains stored field
        try (var searcher = indexService.getShard(0).acquireSearcher(INDEX)) {
            try (var indexReader = searcher.getIndexReader()) {
                var document = indexReader.storedFields().document(0);
                assertEquals(document.getField("field_pattern_text.stored").binaryValue().utf8ToString(), message);
            }
        }
    }

    public void testSmallValueNotStored() throws IOException {
        var createRequest = indicesAdmin().prepareCreate(INDEX).setSettings(LOGSDB_SETTING).setMapping(mapping);
        IndexService indexService = createIndex(INDEX, createRequest);

        // small message
        String message = randomMessage();
        List<String> messages = List.of(message);
        indexDocs(messages);

        assertMappings();
        assertMessagesInSource(messages);

        // assert only contains stored field if templating is disabled
        try (var searcher = indexService.getShard(0).acquireSearcher(INDEX)) {
            try (var indexReader = searcher.getIndexReader()) {
                var document = indexReader.storedFields().document(0);
                if (disableTemplating) {
                    assertEquals(document.getField("field_pattern_text.stored").binaryValue().utf8ToString(), message);
                } else {
                    assertNull(document.getField("field_pattern_text.stored"));
                }
            }
        }
    }

    public void testPhraseQuery() throws IOException {
        var createRequest = new CreateIndexRequest(INDEX).mapping(mapping);
        createRequest.settings(LOGSDB_SETTING);
        assertAcked(admin().indices().create(createRequest));

        String smallMessage = "cat dog 123 house mouse";
        final String message = randomBoolean() ? smallMessage : smallMessage.repeat(32_000 / smallMessage.length());

        List<String> logMessages = List.of(message);
        indexDocs(logMessages);
        assertMappings();

        var query = QueryBuilders.matchPhraseQuery("field_pattern_text", "dog 123 house");
        var searchRequest = client().prepareSearch(INDEX).setQuery(query);

        assertNoFailuresAndResponse(searchRequest, searchResponse -> { assertEquals(1, searchResponse.getHits().getTotalHits().value()); });
    }

    public void testQueryResultsSameAsMatchOnlyText() throws IOException {
        var createRequest = new CreateIndexRequest(INDEX).mapping(mapping);

        if (randomBoolean()) {
            createRequest.settings(LOGSDB_SETTING);
        }

        assertAcked(admin().indices().create(createRequest));

        int numDocs = randomIntBetween(10, 200);
        List<String> logMessages = generateMessages(numDocs);
        indexDocs(logMessages);
        assertMappings();

        var queryTerms = logMessages.stream().flatMap(m -> randomQueryValues(m).stream()).toList();
        {
            var ptQueries = buildQueries(PATTERN_TEXT_FIELD, queryTerms, QueryBuilders::matchPhraseQuery);
            var motQueries = buildQueries(MATCH_ONLY_TEXT_FIELD, queryTerms, QueryBuilders::matchPhraseQuery);
            assertQueryResults(ptQueries, motQueries, numDocs, "phrase");
        }
        {
            var ptQueries = buildQueries(PATTERN_TEXT_FIELD, queryTerms, QueryBuilders::matchQuery);
            var motQueries = buildQueries(MATCH_ONLY_TEXT_FIELD, queryTerms, QueryBuilders::matchQuery);
            assertQueryResults(ptQueries, motQueries, numDocs, "match");
        }
        {
            var ptQueries = buildQueries(PATTERN_TEXT_FIELD, queryTerms, QueryBuilders::termQuery);
            var motQueries = buildQueries(MATCH_ONLY_TEXT_FIELD, queryTerms, QueryBuilders::termQuery);
            assertQueryResults(ptQueries, motQueries, numDocs, "term");
        }
    }

    private void assertQueryResults(
        List<QueryBuilder> patternTextQueries,
        List<QueryBuilder> matchOnlyTextQueries,
        int numDocs,
        String queryType
    ) {
        var numQueriesWithResults = new AtomicInteger(0);
        var numQueriesTotal = new AtomicInteger(0);
        for (int i = 0; i < patternTextQueries.size(); ++i) {
            var ptRequest = client().prepareSearch(INDEX).setQuery(patternTextQueries.get(i)).setSize(numDocs);
            var motRequest = client().prepareSearch(INDEX).setQuery(matchOnlyTextQueries.get(i)).setSize(numDocs);

            numQueriesTotal.incrementAndGet();
            assertNoFailuresAndResponse(ptRequest, ptResponse -> {
                assertNoFailuresAndResponse(motRequest, motResponse -> {

                    assertEquals(motResponse.getHits().getTotalHits().value(), ptResponse.getHits().getTotalHits().value());

                    var motDocIds = Arrays.stream(motResponse.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toSet());
                    var ptDocIds = Arrays.stream(ptResponse.getHits().getHits()).map(SearchHit::getId).collect(Collectors.toSet());
                    assertEquals(motDocIds, ptDocIds);

                    if (motResponse.getHits().getTotalHits().value() > 0) {
                        numQueriesWithResults.incrementAndGet();
                    }
                });
            });
        }
        logger.info("Ran {} {} queries, of which {} had matches", numQueriesTotal.get(), queryType, numQueriesWithResults.get());
    }

    private List<QueryBuilder> buildQueries(String field, List<String> terms, BiFunction<String, Object, QueryBuilder> queryBuilder) {
        return terms.stream().map(t -> queryBuilder.apply(field, t)).toList();
    }

    public static List<String> randomMessagesOfVariousSizes(int numDocs) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            String message = randomFrom(
                random(),
                // no value for message
                () -> null,
                // regular small message, stored in doc values
                PatternTextIntegrationTests::randomMessage,
                // large value, needs to be put in stored field
                () -> randomMessage(8 * 1024)
            );
            messages.add(message);
        }
        return messages;
    }

    private static List<String> randomQueryValues(String value) {
        var values = new ArrayList<String>();

        values.add(value);
        values.add(randomSubstring(value));

        var tokenizerRegex = "[\\s\\p{Punct}]+";
        List<String> tokens = Arrays.stream(value.split(tokenizerRegex)).filter(t -> t.isEmpty() == false).toList();
        if (tokens.isEmpty() == false) {
            values.add(randomFrom(tokens));
            values.add(randomSubPhrase(tokens));
        }
        return values;
    }

    private static String randomSubstring(String value) {
        int low = ESTestCase.randomIntBetween(0, value.length() - 1);
        int hi = ESTestCase.randomIntBetween(low + 1, value.length());
        return value.substring(low, hi);
    }

    private static String randomSubPhrase(List<String> tokens) {
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

            final XContentBuilder xContentBuilder;
            if (msg == null) {
                xContentBuilder = JsonXContent.contentBuilder().startObject().field("@timestamp", timestamp).endObject();
            } else {
                xContentBuilder = JsonXContent.contentBuilder()
                    .startObject()
                    .field("@timestamp", timestamp)
                    .field("field_pattern_text", msg)
                    .field("field_match_only_text", msg)
                    .endObject();
            }

            var indexRequest = new IndexRequest(INDEX).opType(DocWriteRequest.OpType.CREATE).source(xContentBuilder);
            bulkRequest.add(indexRequest);
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertFalse(bulkResponse.hasFailures());
        safeGet(indicesAdmin().refresh(new RefreshRequest(INDEX).indicesOptions(IndicesOptions.lenientExpandOpenHidden())));
    }

    public static String randomMessage(int minLength) {
        StringBuilder sb = new StringBuilder();
        while (sb.length() < minLength) {
            sb.append(randomMessage());
        }
        return sb.toString();
    }

    public static String randomMessageMaybeLarge() {
        if (randomDouble() < 0.2) {
            return randomMessage(32 * 1024);
        } else {
            return randomMessage();
        }
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
                    PatternTextIntegrationTests::randomTimestamp,
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
        // The random millis are below year 10000 in UTC, but if the date is within 1 day of year 10000, the year can be 10000 in the
        // selected timezone. Since the date formatter cannot handle years greater than 9999, select another date.
        var zonedDateTime = randomValueOtherThanMany(
            t -> t.getYear() == 10000,
            () -> ZonedDateTime.ofInstant(Instant.ofEpochMilli(randomMillisUpToYear9999()), randomZone())
        );
        DateFormatter formatter = DateFormatter.forPattern(randomDateFormatterPattern()).withLocale(randomLocale(random()));
        return formatter.format(zonedDateTime);
    }

    @SuppressWarnings("unchecked")
    public void assertMappings() {
        Map<String, Object> mappings = indicesAdmin().prepareGetMappings(TEST_REQUEST_TIMEOUT, INDEX)
            .get()
            .mappings()
            .get(INDEX)
            .sourceAsMap();
        Map<String, Object> properties = (Map<String, Object>) mappings.get("properties");
        var patternText = (Map<String, Object>) properties.get("field_pattern_text");
        assertEquals("pattern_text", patternText.get("type"));

        var matchOnlyText = (Map<String, Object>) properties.get("field_match_only_text");
        assertEquals("match_only_text", matchOnlyText.get("type"));
    }

    void assertMessagesInSource(List<String> logMessages) {
        var request = client().prepareSearch(INDEX).setSize(100).setFetchSource(true);
        assertNoFailuresAndResponse(request, response -> {
            assertEquals(logMessages.size(), response.getHits().getHits().length);
            var values = new HashSet<>(
                Arrays.stream(response.getHits().getHits()).map(SearchHit::getSourceAsMap).map(m -> m.get(PATTERN_TEXT_FIELD)).toList()
            );

            assertEquals(new HashSet<>(logMessages), values);
        });
    }
}
