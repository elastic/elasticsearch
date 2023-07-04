/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MinPrimaryShardDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.action.admin.indices.rollover.RolloverConfiguration;
import org.elasticsearch.action.dlm.ExplainIndexDataLifecycle;
import org.elasticsearch.cluster.metadata.DataLifecycle;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.datastreams.lifecycle.action.ExplainDataLifecycleAction.Response;
import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ExplainDataLifecycleResponseTests extends AbstractWireSerializingTestCase<Response> {

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
        DataLifecycle lifecycle = new DataLifecycle();
        ExplainIndexDataLifecycle explainIndex = createRandomIndexDLMExplanation(now, lifecycle);
        explainIndex.setNowSupplier(() -> now);
        {
            Response response = new Response(List.of(explainIndex), null);

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
            assertThat(explainIndexMap.get("managed_by_lifecycle"), is(explainIndex.isManagedByDLM()));
            if (explainIndex.isManagedByDLM()) {
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
                assertThat(explainIndexMap.get("lifecycle"), is(new HashMap<>())); // empty lifecycle
                assertThat(explainIndexMap.get("error"), is(explainIndex.getError()));
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
            Response response = new Response(List.of(explainIndex), new RolloverConfiguration(rolloverConditions));

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
            assertThat(explainIndexMap.get("managed_by_lifecycle"), is(explainIndex.isManagedByDLM()));
            if (explainIndex.isManagedByDLM()) {
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
                assertThat(explainIndexMap.get("error"), is(explainIndex.getError()));

                Map<String, Object> lifecycleRollover = (Map<String, Object>) ((Map<String, Object>) explainIndexMap.get("lifecycle")).get(
                    "rollover"
                );
                assertThat(lifecycleRollover.get("min_primary_shard_docs"), is(4));
                assertThat(lifecycleRollover.get("max_primary_shard_docs"), is(9));
            }
        }
        {
            // Make sure generation_date is not present if it is null (which it is for a write index):
            ExplainIndexDataLifecycle explainIndexWithNullGenerationDate = new ExplainIndexDataLifecycle(
                randomAlphaOfLengthBetween(10, 30),
                true,
                now,
                randomBoolean() ? now + TimeValue.timeValueDays(1).getMillis() : null,
                null,
                lifecycle,
                randomBoolean() ? new NullPointerException("bad times").getMessage() : null
            );
            Response response = new Response(List.of(explainIndexWithNullGenerationDate), null);

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
        DataLifecycle lifecycle = new DataLifecycle();
        Response response = new Response(
            List.of(
                createRandomIndexDLMExplanation(now, lifecycle),
                createRandomIndexDLMExplanation(now, lifecycle),
                createRandomIndexDLMExplanation(now, lifecycle)
            ),
            null
        );

        // 2 chunks are the opening and closing of the json object
        // one chunk / index in response represent the other 3 chunks
        AbstractChunkedSerializingTestCase.assertChunkCount(response, ignored -> 5);
    }

    private static ExplainIndexDataLifecycle createRandomIndexDLMExplanation(long now, @Nullable DataLifecycle lifecycle) {
        return new ExplainIndexDataLifecycle(
            randomAlphaOfLengthBetween(10, 30),
            true,
            now,
            randomBoolean() ? now + TimeValue.timeValueDays(1).getMillis() : null,
            randomBoolean() ? TimeValue.timeValueMillis(now) : null,
            lifecycle,
            randomBoolean() ? new NullPointerException("bad times").getMessage() : null
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
        return new Response(
            List.of(createRandomIndexDLMExplanation(System.nanoTime(), randomBoolean() ? new DataLifecycle() : null)),
            randomBoolean()
                ? new RolloverConfiguration(
                    new RolloverConditions(
                        Map.of(MaxPrimaryShardDocsCondition.NAME, new MaxPrimaryShardDocsCondition(randomLongBetween(1000, 199_999_000)))
                    )
                )
                : null
        );
    }
}
