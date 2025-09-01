/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logsdb.patternedtext;

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
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.license.LicenseSettings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.logsdb.LogsDBPlugin;
import org.junit.Before;

import java.io.IOException;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailuresAndResponse;

public class PatternedTextVsMatchOnlyTextTests extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(PatternedTextVsMatchOnlyTextTests.class);

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(LicenseSettings.SELF_GENERATED_LICENSE_TYPE.getKey(), "trial")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(MapperExtrasPlugin.class, LogsDBPlugin.class, LocalStateCompositeXPackPlugin.class);
    }

    private static final String INDEX = "test_index";
    private static final String MATCH_ONLY_TEXT_FIELD = "field_match_only_text";
    private static final String PATTERNED_TEXT_FIELD = "field_patterned_text";
    private static final String MAPPING_TEMPLATE = """
            {
              "properties": {
                "@timestamp": { "type": "date" },
                "field_match_only_text": { "type": "match_only_text" },
                "field_patterned_text": {
                    "type": "patterned_text",
                    "index_options": "%"
                }
              }
            }
        """;

    private static final String MAPPING_DOCS_ONLY = MAPPING_TEMPLATE.replace("%", "docs");
    private static final String MAPPING_POSITIONS = MAPPING_TEMPLATE.replace("%", "positions");

    @Before
    public void setup() {
        assumeTrue("Only when patterned_text feature flag is enabled", PatternedTextFieldMapper.PATTERNED_TEXT_MAPPER.isEnabled());
    }

    public void testQueries() throws IOException {
        var mapping = randomBoolean() ? MAPPING_DOCS_ONLY : MAPPING_POSITIONS;
        var createRequest = new CreateIndexRequest(INDEX).mapping(mapping);

        assertAcked(admin().indices().create(createRequest));

        int numDocs = randomIntBetween(10, 200);
        List<String> logMessages = generateMessages(numDocs);
        indexDocs(logMessages);

        var queryTerms = logMessages.stream().flatMap(m -> randomQueryValues(m).stream()).toList();
        {
            var ptQueries = buildQueries(PATTERNED_TEXT_FIELD, queryTerms, QueryBuilders::matchPhraseQuery);
            var motQueries = buildQueries(MATCH_ONLY_TEXT_FIELD, queryTerms, QueryBuilders::matchPhraseQuery);
            assertQueryResults(ptQueries, motQueries, numDocs, "phrase");
        }
        {
            var ptQueries = buildQueries(PATTERNED_TEXT_FIELD, queryTerms, QueryBuilders::matchQuery);
            var motQueries = buildQueries(MATCH_ONLY_TEXT_FIELD, queryTerms, QueryBuilders::matchQuery);
            assertQueryResults(ptQueries, motQueries, numDocs, "match");
        }
        {
            var ptQueries = buildQueries(PATTERNED_TEXT_FIELD, queryTerms, QueryBuilders::termQuery);
            var motQueries = buildQueries(MATCH_ONLY_TEXT_FIELD, queryTerms, QueryBuilders::termQuery);
            assertQueryResults(ptQueries, motQueries, numDocs, "term");
        }
    }

    private void assertQueryResults(
        List<QueryBuilder> patternedTextQueries,
        List<QueryBuilder> matchOnlyTextQueries,
        int numDocs,
        String queryType
    ) {
        var numQueriesWithResults = new AtomicInteger(0);
        var numQueriesTotal = new AtomicInteger(0);
        for (int i = 0; i < patternedTextQueries.size(); ++i) {
            var ptRequest = client().prepareSearch(INDEX).setQuery(patternedTextQueries.get(i)).setSize(numDocs);
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
                    PatternedTextVsMatchOnlyTextTests::randomTimestamp,
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
}
