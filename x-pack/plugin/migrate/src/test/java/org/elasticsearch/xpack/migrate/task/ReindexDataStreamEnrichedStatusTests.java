/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.migrate.task;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Map.entry;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;

public class ReindexDataStreamEnrichedStatusTests extends AbstractWireSerializingTestCase<ReindexDataStreamEnrichedStatus> {

    @Override
    protected Writeable.Reader<ReindexDataStreamEnrichedStatus> instanceReader() {
        return ReindexDataStreamEnrichedStatus::new;
    }

    @Override
    protected ReindexDataStreamEnrichedStatus createTestInstance() {
        return new ReindexDataStreamEnrichedStatus(
            randomLong(),
            randomNegativeInt(),
            randomNegativeInt(),
            randomBoolean(),
            nullableTestException(),
            randomInProgressMap(),
            randomNegativeInt(),
            randomErrorList()
        );
    }

    private Map<String, Tuple<Long, Long>> randomInProgressMap() {
        return randomMap(1, 50, () -> Tuple.tuple(randomAlphaOfLength(50), Tuple.tuple(randomNonNegativeLong(), randomNonNegativeLong())));
    }

    private Exception nullableTestException() {
        if (randomBoolean()) {
            return testException();
        }
        return null;
    }

    private Exception testException() {
        /*
         * Unfortunately ElasticsearchException doesn't have an equals and just falls back to Object::equals. So we can't test for equality
         * when we're using an exception. So always just use null.
         */
        return null;
    }

    private List<String> randomList() {
        return randomList(0);
    }

    private List<String> randomList(int minSize) {
        return randomList(minSize, Math.max(minSize, 100), () -> randomAlphaOfLength(50));
    }

    private Set<String> randomSet(int minSize) {
        return randomSet(minSize, Math.max(minSize, 100), () -> randomAlphaOfLength(50));
    }

    private List<Tuple<String, Exception>> randomErrorList() {
        return randomErrorList(0);
    }

    private List<Tuple<String, Exception>> randomErrorList(int minSize) {
        return randomList(minSize, Math.max(minSize, 100), () -> Tuple.tuple(randomAlphaOfLength(30), testException()));
    }

    @Override
    protected ReindexDataStreamEnrichedStatus mutateInstance(ReindexDataStreamEnrichedStatus instance) throws IOException {
        long startTime = instance.persistentTaskStartTime();
        int totalIndices = instance.totalIndices();
        int totalIndicesToBeUpgraded = instance.totalIndicesToBeUpgraded();
        boolean complete = instance.complete();
        Exception exception = instance.exception();
        Map<String, Tuple<Long, Long>> inProgress = instance.inProgress();
        int pending = instance.pending();
        List<Tuple<String, Exception>> errors = instance.errors();
        switch (randomIntBetween(0, 6)) {
            case 0 -> startTime = randomLong();
            case 1 -> totalIndices = totalIndices + 1;
            case 2 -> totalIndicesToBeUpgraded = totalIndicesToBeUpgraded + 1;
            case 3 -> complete = complete == false;
            case 4 -> inProgress = randomInProgressMap();
            case 5 -> pending = pending + 1;
            case 6 -> errors = randomErrorList(errors.size() + 1);
            default -> throw new UnsupportedOperationException();
        }
        return new ReindexDataStreamEnrichedStatus(
            startTime,
            totalIndices,
            totalIndicesToBeUpgraded,
            complete,
            exception,
            inProgress,
            pending,
            errors
        );
    }

    public void testToXContent() throws IOException {
        ReindexDataStreamEnrichedStatus status = new ReindexDataStreamEnrichedStatus(
            1234L,
            200,
            100,
            true,
            new ElasticsearchException("the whole task failed"),
            Map.of("index-1", Tuple.tuple(10L, 8L)),
            8,
            List.of(
                Tuple.tuple("index7", new ElasticsearchException("index7 failed")),
                Tuple.tuple("index8", new ElasticsearchException("index8 " + "failed"))
            )
        );
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)) {
            builder.humanReadable(true);
            status.toXContent(builder, EMPTY_PARAMS);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                Map<String, Object> parserMap = parser.map();
                assertThat(
                    parserMap,
                    equalTo(
                        Map.ofEntries(
                            entry("start_time", "1970-01-01T00:00:01.234Z"),
                            entry("start_time_millis", 1234),
                            entry("total_indices_in_data_stream", 200),
                            entry("total_indices_requiring_upgrade", 100),
                            entry("complete", true),
                            entry("exception", "the whole task failed"),
                            entry("successes", 89),
                            entry("in_progress", List.of(Map.of("index", "index-1", "total_doc_count", 10, "reindexed_doc_count", 8))),
                            entry("pending", 8),
                            entry(
                                "errors",
                                List.of(
                                    Map.of("index", "index7", "message", "index7 failed"),
                                    Map.of("index", "index8", "message", "index8 failed")
                                )
                            )
                        )
                    )
                );
            }
        }
    }
}
