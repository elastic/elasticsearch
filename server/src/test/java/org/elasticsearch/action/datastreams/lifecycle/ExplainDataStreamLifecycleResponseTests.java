/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams.lifecycle;

import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MinPrimaryShardDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.datastreams.lifecycle.ExplainDataStreamLifecycleAction.Response;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ExplainDataStreamLifecycleResponseTests extends AbstractWireSerializingTestCase<Response> {

    private NamedWriteableRegistry namedWriteableRegistry;
    private NamedXContentRegistry xContentRegistry;

    @Before
    public void setupNamedWriteableRegistry() {
        namedWriteableRegistry = new NamedWriteableRegistry(IndicesModule.getNamedWriteables());
        xContentRegistry = new NamedXContentRegistry(IndicesModule.getNamedXContents());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return namedWriteableRegistry;
    }

    @SuppressWarnings("unchecked")
    public void testToXContent() throws IOException {
        long now = System.currentTimeMillis();
        DataStreamLifecycle lifecycle = DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE;
        ExplainIndexDataStreamLifecycle explainIndex = createRandomIndexDataStreamLifecycleExplanation(now, lifecycle);
        explainIndex.setNowSupplier(() -> now);
        {
            Response response = new Response(List.of(explainIndex), null, null, null);

            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            response.toXContentChunked(EMPTY_PARAMS).forEachRemaining(xcontent -> {
                try {
                    xcontent.toXContent(builder, EMPTY_PARAMS);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    fail(e.getMessage());
                }
            });
            Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            Map<String, Object> indices = (Map<String, Object>) xContentMap.get("indices");
            assertThat(indices.size(), is(1));
            Map<String, Object> explainIndexMap = (Map<String, Object>) indices.get(explainIndex.getIndex());
            assertThat(explainIndexMap.get("managed_by_lifecycle"), is(explainIndex.isManagedByLifecycle()));
            if (explainIndex.isManagedByLifecycle()) {
                assertThat(explainIndexMap.get("index_creation_date_millis"), is(explainIndex.getIndexCreationDate()));
                assertThat(
                    explainIndexMap.get("time_since_index_creation"),
                    is(explainIndex.getTimeSinceIndexCreation(() -> now).toHumanReadableString(2))
                );
                if (explainIndex.getRolloverDate() != null) {
                    assertThat(explainIndexMap.get("rollover_date_millis"), is(explainIndex.getRolloverDate()));
                    assertThat(
                        explainIndexMap.get("time_since_rollover"),
                        is(explainIndex.getTimeSinceRollover(() -> now).toHumanReadableString(2))
                    );
                }
                if (explainIndex.getGenerationTime(() -> now) != null) {
                    assertThat(
                        explainIndexMap.get("generation_time"),
                        is(explainIndex.getGenerationTime(() -> now).toHumanReadableString(2))
                    );
                } else {
                    assertThat(explainIndexMap.get("generation_time"), is(nullValue()));
                }
                assertThat(explainIndexMap.get("lifecycle"), is(Map.of("enabled", true)));
                if (explainIndex.getError() != null) {
                    Map<String, Object> errorObject = (Map<String, Object>) explainIndexMap.get("error");
                    assertThat(errorObject.get(ErrorEntry.MESSAGE_FIELD.getPreferredName()), is(explainIndex.getError().error()));
                    assertThat(
                        errorObject.get(ErrorEntry.FIRST_OCCURRENCE_MILLIS_FIELD.getPreferredName()),
                        is(explainIndex.getError().firstOccurrenceTimestamp())
                    );
                    assertThat(
                        errorObject.get(ErrorEntry.LAST_RECORDED_MILLIS_FIELD.getPreferredName()),
                        is(explainIndex.getError().recordedTimestamp())
                    );
                    assertThat(errorObject.get(ErrorEntry.RETRY_COUNT_FIELD.getPreferredName()), is(explainIndex.getError().retryCount()));
                } else {
                    assertThat(explainIndexMap.get("error"), is(nullValue()));
                }
            }
        }

        {
            // let's add some rollover conditions (i.e. include defaults)
            RolloverConditions rolloverConditions = new RolloverConditions(
                Map.of(
                    MaxPrimaryShardDocsCondition.NAME,
                    new MaxPrimaryShardDocsCondition(9L),
                    MinPrimaryShardDocsCondition.NAME,
                    new MinPrimaryShardDocsCondition(4L)
                )
            );
            DataStreamGlobalRetention dataGlobalRetention = DataStreamTestHelper.randomGlobalRetention();
            DataStreamGlobalRetention failuresGlobalRetention = new DataStreamGlobalRetention(
                randomTimeValue(1, 30, TimeUnit.DAYS),
                dataGlobalRetention == null ? null : dataGlobalRetention.maxRetention()
            );
            Response response = new Response(
                List.of(explainIndex),
                new RolloverConfiguration(rolloverConditions),
                dataGlobalRetention,
                failuresGlobalRetention
            );

            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            response.toXContentChunked(EMPTY_PARAMS).forEachRemaining(xcontent -> {
                try {
                    xcontent.toXContent(builder, EMPTY_PARAMS);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    fail(e.getMessage());
                }
            });
            Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            Map<String, Object> indices = (Map<String, Object>) xContentMap.get("indices");
            assertThat(indices.size(), is(1));
            Map<String, Object> explainIndexMap = (Map<String, Object>) indices.get(explainIndex.getIndex());
            assertThat(explainIndexMap.get("index"), is(explainIndex.getIndex()));
            assertThat(explainIndexMap.get("managed_by_lifecycle"), is(explainIndex.isManagedByLifecycle()));
            if (explainIndex.isManagedByLifecycle()) {
                assertThat(explainIndexMap.get("index_creation_date_millis"), is(explainIndex.getIndexCreationDate()));
                assertThat(
                    explainIndexMap.get("time_since_index_creation"),
                    is(explainIndex.getTimeSinceIndexCreation(() -> now).toHumanReadableString(2))
                );
                if (explainIndex.getRolloverDate() != null) {
                    assertThat(explainIndexMap.get("rollover_date_millis"), is(explainIndex.getRolloverDate()));
                    assertThat(
                        explainIndexMap.get("time_since_rollover"),
                        is(explainIndex.getTimeSinceRollover(() -> now).toHumanReadableString(2))
                    );
                }
                if (explainIndex.getGenerationTime(() -> now) != null) {
                    assertThat(
                        explainIndexMap.get("generation_time"),
                        is(explainIndex.getGenerationTime(() -> now).toHumanReadableString(2))
                    );
                } else {
                    assertThat(explainIndexMap.get("generation_time"), is(nullValue()));
                }
                if (explainIndex.getError() != null) {
                    Map<String, Object> errorObject = (Map<String, Object>) explainIndexMap.get("error");
                    assertThat(errorObject.get(ErrorEntry.MESSAGE_FIELD.getPreferredName()), is(explainIndex.getError().error()));
                    assertThat(
                        errorObject.get(ErrorEntry.FIRST_OCCURRENCE_MILLIS_FIELD.getPreferredName()),
                        is(explainIndex.getError().firstOccurrenceTimestamp())
                    );
                    assertThat(
                        errorObject.get(ErrorEntry.LAST_RECORDED_MILLIS_FIELD.getPreferredName()),
                        is(explainIndex.getError().recordedTimestamp())
                    );
                    assertThat(errorObject.get(ErrorEntry.RETRY_COUNT_FIELD.getPreferredName()), is(explainIndex.getError().retryCount()));
                } else {
                    assertThat(explainIndexMap.get("error"), is(nullValue()));
                }

                Map<String, Object> lifecycleMap = (Map<String, Object>) explainIndexMap.get("lifecycle");
                assertThat(lifecycleMap.get("data_retention"), nullValue());

                Map<String, Object> lifecycleRollover = (Map<String, Object>) lifecycleMap.get("rollover");
                assertThat(lifecycleRollover.get("min_primary_shard_docs"), is(4));
                assertThat(lifecycleRollover.get("max_primary_shard_docs"), is(9));
            }
        }
        {
            // Make sure generation_date is not present if it is null (which it is for a write index):
            String index = randomAlphaOfLengthBetween(10, 30);
            ExplainIndexDataStreamLifecycle explainIndexWithNullGenerationDate = new ExplainIndexDataStreamLifecycle(
                index,
                true,
                randomBoolean(),
                now,
                randomBoolean() ? now + TimeValue.timeValueDays(1).getMillis() : null,
                null,
                lifecycle,
                randomBoolean()
                    ? new ErrorEntry(
                        System.currentTimeMillis(),
                        new NullPointerException("bad times").getMessage(),
                        System.currentTimeMillis(),
                        randomIntBetween(0, 30)
                    )
                    : null
            );
            Response response = new Response(List.of(explainIndexWithNullGenerationDate), null, null, null);

            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            response.toXContentChunked(EMPTY_PARAMS).forEachRemaining(xcontent -> {
                try {
                    xcontent.toXContent(builder, EMPTY_PARAMS);
                } catch (IOException e) {
                    logger.error(e.getMessage(), e);
                    fail(e.getMessage());
                }
            });
            Map<String, Object> xContentMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            Map<String, Object> indices = (Map<String, Object>) xContentMap.get("indices");
            assertThat(indices.size(), is(1));
            Map<String, Object> explainIndexMap = (Map<String, Object>) indices.get(explainIndexWithNullGenerationDate.getIndex());
            assertThat(explainIndexMap.get("managed_by_lifecycle"), is(true));
            assertThat(explainIndexMap.get("generation_time"), is(nullValue()));
        }
    }

    public void testChunkCount() {
        long now = System.currentTimeMillis();
        DataStreamLifecycle lifecycle = DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE;
        Response response = new Response(
            List.of(
                createRandomIndexDataStreamLifecycleExplanation(now, lifecycle),
                createRandomIndexDataStreamLifecycleExplanation(now, lifecycle),
                createRandomIndexDataStreamLifecycleExplanation(now, lifecycle)
            ),
            null,
            null,
            null
        );

        // 2 chunks are the opening and closing of the json object
        // one chunk / index in response represent the other 3 chunks
        AbstractChunkedSerializingTestCase.assertChunkCount(response, ignored -> 5);
    }

    private static ExplainIndexDataStreamLifecycle createRandomIndexDataStreamLifecycleExplanation(
        long now,
        @Nullable DataStreamLifecycle lifecycle
    ) {
        String index = randomAlphaOfLengthBetween(10, 30);
        return new ExplainIndexDataStreamLifecycle(
            index,
            true,
            randomBoolean(),
            now,
            randomBoolean() ? now + TimeValue.timeValueDays(1).getMillis() : null,
            randomBoolean() ? TimeValue.timeValueMillis(now) : null,
            lifecycle,
            randomBoolean()
                ? new ErrorEntry(
                    System.currentTimeMillis(),
                    new NullPointerException("bad times").getMessage(),
                    System.currentTimeMillis(),
                    randomIntBetween(0, 30)
                )
                : null
        );
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }

    @Override
    protected Response createTestInstance() {
        return randomResponse();
    }

    @Override
    protected Response mutateInstance(Response instance) {
        return randomResponse();
    }

    private Response randomResponse() {
        var dataGlobalRetention = DataStreamGlobalRetention.create(
            randomBoolean() ? TimeValue.timeValueDays(randomIntBetween(1, 10)) : null,
            randomBoolean() ? TimeValue.timeValueDays(randomIntBetween(10, 20)) : null
        );
        var failuresGlobalRetention = DataStreamGlobalRetention.create(
            randomBoolean() ? TimeValue.timeValueDays(randomIntBetween(1, 10)) : null,
            dataGlobalRetention == null ? null : dataGlobalRetention.maxRetention()
        );
        return new Response(
            List.of(
                createRandomIndexDataStreamLifecycleExplanation(
                    System.nanoTime(),
                    randomBoolean() ? DataStreamLifecycle.DEFAULT_DATA_LIFECYCLE : null
                )
            ),
            randomBoolean()
                ? new RolloverConfiguration(
                    new RolloverConditions(
                        Map.of(MaxPrimaryShardDocsCondition.NAME, new MaxPrimaryShardDocsCondition(randomLongBetween(1000, 199_999_000)))
                    )
                )
                : null,
            dataGlobalRetention,
            failuresGlobalRetention
        );
    }
}
