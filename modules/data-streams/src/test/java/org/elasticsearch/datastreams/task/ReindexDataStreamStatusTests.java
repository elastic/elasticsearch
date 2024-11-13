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

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.equalTo;

public class ReindexDataStreamStatusTests extends AbstractWireSerializingTestCase<ReindexDataStreamStatus> {

    @Override
    protected Writeable.Reader<ReindexDataStreamStatus> instanceReader() {
        return ReindexDataStreamStatus::new;
    }

    @Override
    protected ReindexDataStreamStatus createTestInstance() {
        return new ReindexDataStreamStatus(randomBoolean(), nullableTestException(), randomList(), randomList(), randomList(), randomMap());
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

    private Map<String, Exception> randomMap() {
        return randomMap(0);
    }

    private Map<String, Exception> randomMap(int minSize) {
        return randomMap(minSize, Math.max(minSize, 10), () -> Tuple.tuple(randomAlphaOfLength(50), testException()));
    }

    @Override
    protected ReindexDataStreamStatus mutateInstance(ReindexDataStreamStatus instance) throws IOException {
        boolean complete = instance.complete();
        Exception exception = instance.exception();
        List<String> successes = instance.successes();
        List<String> inProgress = instance.inProgress();
        List<String> pending = instance.pending();
        Map<String, Exception> errors = instance.errors();
        switch (randomIntBetween(0, 4)) {
            case 0 -> complete = complete == false;
            case 1 -> successes = randomList(successes.size() + 1);
            case 2 -> inProgress = randomList(inProgress.size() + 1);
            case 3 -> pending = randomList(pending.size() + 1);
            case 4 -> errors = randomMap(errors.size() + 1);
            default -> throw new UnsupportedOperationException();
        }
        return new ReindexDataStreamStatus(complete, exception, successes, inProgress, pending, errors);
    }

    public void testToXContent() throws IOException {
        ReindexDataStreamStatus status = new ReindexDataStreamStatus(
            true,
            new ElasticsearchException("the whole task failed"),
            List.of("index1", "index2"),
            List.of("index3", "index4"),
            List.of("index5", "index6"),
            Map.of("index7", new ElasticsearchException("index7 failed"), "index8", new ElasticsearchException("index8 failed"))
        );
        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent)) {
            builder.humanReadable(true);
            status.toXContent(builder, EMPTY_PARAMS);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                assertThat(
                    parser.map(),
                    equalTo(
                        Map.of(
                            "complete",
                            true,
                            "exception",
                            "the whole task failed",
                            "successes",
                            Map.of("count", 2, "indices", List.of("index1", "index2")),
                            "in_progress",
                            Map.of("count", 2, "indices", List.of("index3", "index4")),
                            "pending",
                            Map.of("count", 2, "indices", List.of("index5", "index6")),
                            "errors",
                            Map.of("count", 2, "indices", Map.of("index7", "index7 failed", "index8", "index8 failed"))
                        )
                    )
                );
            }
        }
    }
}
