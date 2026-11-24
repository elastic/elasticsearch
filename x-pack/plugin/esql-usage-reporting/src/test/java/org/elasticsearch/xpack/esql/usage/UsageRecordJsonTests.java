/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.usage;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener.Outcome;

import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.usage.HostedUsageRecordBuilderTests.CREATION_TIME;
import static org.elasticsearch.xpack.esql.usage.HostedUsageRecordBuilderTests.DRAIN_TIME;
import static org.elasticsearch.xpack.esql.usage.HostedUsageRecordBuilderTests.IDENTITY;
import static org.elasticsearch.xpack.esql.usage.HostedUsageRecordBuilderTests.PERIOD;
import static org.hamcrest.Matchers.equalTo;

/**
 * Checks the JSON of the records against the files of {@code usage-vectors}, which document the inputs that produce each one (see the
 * README there) and are meant to be loaded by other implementations of the same records.
 */
public class UsageRecordJsonTests extends ESTestCase {

    private static List<UsageRecord> build(Map<Outcome, CpuComponents> byOutcome) {
        return new HostedUsageRecordBuilder().build(new UsageSnapshot(DRAIN_TIME, byOutcome), IDENTITY, PERIOD, CREATION_TIME);
    }

    private static void assertMatchesVector(String file, Map<Outcome, CpuComponents> byOutcome) throws IOException {
        BytesReference expected;
        try (InputStream in = UsageRecordJsonTests.class.getResourceAsStream("/usage-vectors/" + file)) {
            assertNotNull("missing vector " + file, in);
            expected = new BytesArray(in.readAllBytes());
        }
        assertThat(file, parse(UsageRecordJson.toJson(build(byOutcome))), equalTo(parse(expected)));
    }

    private static List<Object> parse(BytesReference json) throws IOException {
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json.streamInput())) {
            assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_ARRAY));
            return parser.list();
        }
    }

    public void testBilled() throws IOException {
        assertMatchesVector("billed.json", Map.of(Outcome.SUCCESS, new CpuComponents(0, 0, 2_500_999_999L, 0)));
    }

    public void testExemptFailed() throws IOException {
        assertMatchesVector("exempt-failed.json", Map.of(Outcome.FAILED, new CpuComponents(0, 0, 3_000_000_000L, 0)));
    }

    public void testExemptCancelled() throws IOException {
        assertMatchesVector("exempt-cancelled.json", Map.of(Outcome.CANCELLED, new CpuComponents(1_500_000, 0, 0, 0)));
    }

    public void testMultiComponent() throws IOException {
        assertMatchesVector(
            "multi-component.json",
            Map.of(Outcome.SUCCESS, new CpuComponents(1_000_000, 2_000_000, 3_000_000_000L, 4_000_000))
        );
    }

    public void testEmptyBatchIsAnEmptyArray() throws IOException {
        assertThat(parse(UsageRecordJson.toJson(List.of())), equalTo(List.of()));
    }

    public void testSeveralRecordsAreOneArray() throws IOException {
        List<UsageRecord> records = build(
            Map.of(
                Outcome.SUCCESS,
                new CpuComponents(0, 0, 10_000_000, 0),
                Outcome.FAILED,
                new CpuComponents(0, 0, 20_000_000, 0),
                Outcome.CANCELLED,
                new CpuComponents(0, 0, 30_000_000, 0)
            )
        );
        List<Object> parsed = parse(UsageRecordJson.toJson(records));
        assertThat(parsed.size(), equalTo(3));
        for (Object o : parsed) {
            @SuppressWarnings("unchecked")
            Map<String, Object> json = (Map<String, Object>) o; // the parser returns JSON objects as maps
            @SuppressWarnings("unchecked")
            Map<String, Object> source = (Map<String, Object>) json.get("source");
            assertTrue(((String) json.get("id")).startsWith((String) source.get("id")));
        }
    }

    public void testTimestampsHaveMillisecondPrecisionWhateverTheInstant() throws IOException {
        UsageRecord record = new UsageRecord(
            "id",
            Instant.parse("2026-11-23T10:00:00Z"),
            Instant.parse("2026-11-23T10:00:01.5Z"),
            UsageTypes.EXTERNAL_SOURCES_CPU,
            1,
            300,
            Map.of(),
            null,
            IDENTITY
        );
        @SuppressWarnings("unchecked")
        Map<String, Object> json = (Map<String, Object>) parse(UsageRecordJson.toJson(List.of(record))).get(0);
        assertThat(json.get("usage_timestamp"), equalTo("2026-11-23T10:00:00.000Z"));
        assertThat(json.get("creation_timestamp"), equalTo("2026-11-23T10:00:01.500Z"));
    }
}
