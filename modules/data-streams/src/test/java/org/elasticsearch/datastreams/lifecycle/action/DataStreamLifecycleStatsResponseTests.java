/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class DataStreamLifecycleStatsResponseTests extends AbstractWireSerializingTestCase<GetDataStreamLifecycleStatsAction.Response> {

    @Override
    protected GetDataStreamLifecycleStatsAction.Response createTestInstance() {
        boolean hasRun = usually();
        var runDuration = hasRun ? randomLongBetween(10, 100000000) : null;
        var timeBetweenStarts = hasRun && usually() ? randomLongBetween(10, 100000000) : null;
        var dataStreams = IntStream.range(0, randomInt(10))
            .mapToObj(
                ignored -> new GetDataStreamLifecycleStatsAction.Response.DataStreamStats(
                    randomAlphaOfLength(10),
                    randomIntBetween(1, 1000),
                    randomIntBetween(0, 100)
                )
            )
            .toList();
        return new GetDataStreamLifecycleStatsAction.Response(runDuration, timeBetweenStarts, dataStreams);
    }

    @Override
    protected GetDataStreamLifecycleStatsAction.Response mutateInstance(GetDataStreamLifecycleStatsAction.Response instance) {
        var runDuration = instance.getRunDuration();
        var timeBetweenStarts = instance.getTimeBetweenStarts();
        var dataStreams = instance.getDataStreamStats();
        switch (randomInt(2)) {
            case 0 -> runDuration = runDuration != null && randomBoolean()
                ? null
                : randomValueOtherThan(runDuration, () -> randomLongBetween(10, 100000000));
            case 1 -> timeBetweenStarts = timeBetweenStarts != null && randomBoolean()
                ? null
                : randomValueOtherThan(timeBetweenStarts, () -> randomLongBetween(10, 100000000));
            default -> dataStreams = mutateDataStreamStats(dataStreams);
        }
        return new GetDataStreamLifecycleStatsAction.Response(runDuration, timeBetweenStarts, dataStreams);
    }

    private List<GetDataStreamLifecycleStatsAction.Response.DataStreamStats> mutateDataStreamStats(
        List<GetDataStreamLifecycleStatsAction.Response.DataStreamStats> dataStreamStats
    ) {
        // change the stats of a data stream
        List<GetDataStreamLifecycleStatsAction.Response.DataStreamStats> mutated = new ArrayList<>(dataStreamStats);
        if (randomBoolean() && dataStreamStats.isEmpty() == false) {
            int i = randomInt(dataStreamStats.size() - 1);
            GetDataStreamLifecycleStatsAction.Response.DataStreamStats instance = dataStreamStats.get(i);
            mutated.set(i, switch (randomInt(2)) {
                case 0 -> new GetDataStreamLifecycleStatsAction.Response.DataStreamStats(
                    instance.dataStreamName() + randomAlphaOfLength(2),
                    instance.backingIndicesInTotal(),
                    instance.backingIndicesInError()
                );
                case 1 -> new GetDataStreamLifecycleStatsAction.Response.DataStreamStats(
                    instance.dataStreamName(),
                    instance.backingIndicesInTotal() + randomIntBetween(1, 10),
                    instance.backingIndicesInError()
                );
                default -> new GetDataStreamLifecycleStatsAction.Response.DataStreamStats(
                    instance.dataStreamName(),
                    instance.backingIndicesInTotal(),
                    instance.backingIndicesInError() + randomIntBetween(1, 10)
                );

            });
        } else if (dataStreamStats.isEmpty() || randomBoolean()) {
            mutated.add(
                new GetDataStreamLifecycleStatsAction.Response.DataStreamStats(
                    randomAlphaOfLength(10),
                    randomIntBetween(1, 1000),
                    randomIntBetween(0, 100)
                )
            );
        } else {
            mutated.remove(randomInt(dataStreamStats.size() - 1));
        }
        return mutated;
    }

    @SuppressWarnings("unchecked")
    public void testXContentSerialization() throws IOException {
        GetDataStreamLifecycleStatsAction.Response testInstance = createTestInstance();
        try (XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent())) {
            builder.humanReadable(true);
            testInstance.toXContentChunked(ToXContent.EMPTY_PARAMS).forEachRemaining(xcontent -> {
                try {
                    xcontent.toXContent(builder, EMPTY_PARAMS);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    fail(e.getMessage());
                }
            });
            Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            if (testInstance.getRunDuration() == null) {
                assertThat(xContentMap.get("last_run_duration_in_millis"), nullValue());
                assertThat(xContentMap.get("last_run_duration"), nullValue());
            } else {
                assertThat(xContentMap.get("last_run_duration_in_millis"), is(testInstance.getRunDuration().intValue()));
                assertThat(
                    xContentMap.get("last_run_duration"),
                    is(TimeValue.timeValueMillis(testInstance.getRunDuration()).toHumanReadableString(2))
                );
            }

            if (testInstance.getTimeBetweenStarts() == null) {
                assertThat(xContentMap.get("time_between_starts_in_millis"), nullValue());
                assertThat(xContentMap.get("time_between_starts"), nullValue());
            } else {
                assertThat(xContentMap.get("time_between_starts_in_millis"), is(testInstance.getTimeBetweenStarts().intValue()));
                assertThat(
                    xContentMap.get("time_between_starts"),
                    is(TimeValue.timeValueMillis(testInstance.getTimeBetweenStarts()).toHumanReadableString(2))
                );
            }
            assertThat(xContentMap.get("data_stream_count"), is(testInstance.getDataStreamStats().size()));
            List<Map<String, Object>> dataStreams = (List<Map<String, Object>>) xContentMap.get("data_streams");
            if (testInstance.getDataStreamStats().isEmpty()) {
                assertThat(dataStreams.isEmpty(), is(true));
            } else {
                assertThat(dataStreams.size(), is(testInstance.getDataStreamStats().size()));
                for (int i = 0; i < dataStreams.size(); i++) {
                    assertThat(dataStreams.get(i).get("name"), is(testInstance.getDataStreamStats().get(i).dataStreamName()));
                    assertThat(
                        dataStreams.get(i).get("backing_indices_in_total"),
                        is(testInstance.getDataStreamStats().get(i).backingIndicesInTotal())
                    );
                    assertThat(
                        dataStreams.get(i).get("backing_indices_in_error"),
                        is(testInstance.getDataStreamStats().get(i).backingIndicesInError())
                    );
                }
            }
        }
    }

    @Override
    protected Writeable.Reader<GetDataStreamLifecycleStatsAction.Response> instanceReader() {
        return GetDataStreamLifecycleStatsAction.Response::new;
    }
}
