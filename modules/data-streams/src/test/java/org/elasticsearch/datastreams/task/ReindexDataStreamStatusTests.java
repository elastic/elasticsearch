/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.task;

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

import static java.util.Map.entry;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;

public class ReindexDataStreamStatusTests extends AbstractWireSerializingTestCase<ReindexDataStreamStatus> {

    @Override
    protected Writeable.Reader<ReindexDataStreamStatus> instanceReader() {
        return ReindexDataStreamStatus::new;
    }

    @Override
    protected ReindexDataStreamStatus createTestInstance() {
        return new ReindexDataStreamStatus(
            randomLong(),
            randomNegativeInt(),
            randomNegativeInt(),
            randomBoolean(),
            nullableTestException(),
            randomNegativeInt(),
            randomNegativeInt(),
            randomErrorList()
        );
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

    private List<Tuple<String, Exception>> randomErrorList() {
        return randomErrorList(0);
    }

    private List<Tuple<String, Exception>> randomErrorList(int minSize) {
        return randomList(minSize, Math.max(minSize, 100), () -> Tuple.tuple(randomAlphaOfLength(30), testException()));
    }

    @Override
    protected ReindexDataStreamStatus mutateInstance(ReindexDataStreamStatus instance) throws IOException {
        long startTime = instance.persistentTaskStartTime();
        int totalIndices = instance.totalIndices();
        int totalIndicesToBeUpgraded = instance.totalIndicesToBeUpgraded();
        boolean complete = instance.complete();
        Exception exception = instance.exception();
        int inProgress = instance.inProgress();
        int pending = instance.pending();
        List<Tuple<String, Exception>> errors = instance.errors();
        switch (randomIntBetween(0, 6)) {
            case 0 -> startTime = randomLong();
            case 1 -> totalIndices = totalIndices + 1;
            case 2 -> totalIndicesToBeUpgraded = totalIndicesToBeUpgraded + 1;
            case 3 -> complete = complete == false;
            case 4 -> inProgress = inProgress + 1;
            case 5 -> pending = pending + 1;
            case 6 -> errors = randomErrorList(errors.size() + 1);
            default -> throw new UnsupportedOperationException();
        }
        return new ReindexDataStreamStatus(
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
        ReindexDataStreamStatus status = new ReindexDataStreamStatus(
            1234L,
            200,
            100,
            true,
            new ElasticsearchException("the whole task failed"),
            12,
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
                            entry("start_time", 1234),
                            entry("total_indices", 200),
                            entry("total_indices_requiring_upgrade", 100),
                            entry("complete", true),
                            entry("exception", "the whole task failed"),
                            entry("successes", 78),
                            entry("in_progress", 12),
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
