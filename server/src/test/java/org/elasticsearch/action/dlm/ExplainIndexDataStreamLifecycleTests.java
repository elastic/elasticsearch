/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.dlm;

import org.elasticsearch.action.datastreams.lifecycle.ExplainIndexDataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ExplainIndexDataStreamLifecycleTests extends AbstractWireSerializingTestCase<ExplainIndexDataStreamLifecycle> {

    public void testGetGenerationTime() {
        long now = System.currentTimeMillis();
        {
            ExplainIndexDataStreamLifecycle explainIndexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                randomAlphaOfLengthBetween(10, 30),
                true,
                now,
                randomBoolean() ? now + TimeValue.timeValueDays(1).getMillis() : null,
                null,
                new DataStreamLifecycle(),
                randomBoolean() ? new NullPointerException("bad times").getMessage() : null
            );
            assertThat(explainIndexDataStreamLifecycle.getGenerationTime(() -> now + 50L), is(nullValue()));
            explainIndexDataStreamLifecycle = new ExplainIndexDataStreamLifecycle(
                randomAlphaOfLengthBetween(10, 30),
                true,
                now,
                randomBoolean() ? now + TimeValue.timeValueDays(1).getMillis() : null,
                TimeValue.timeValueMillis(now + 100),
                new DataStreamLifecycle(),
                randomBoolean() ? new NullPointerException("bad times").getMessage() : null
            );
            assertThat(explainIndexDataStreamLifecycle.getGenerationTime(() -> now + 500L), is(TimeValue.timeValueMillis(400)));
        }
        {
            // null for unmanaged index
            ExplainIndexDataStreamLifecycle indexDataLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                false,
                null,
                null,
                null,
                null,
                null
            );
            assertThat(indexDataLifecycle.getGenerationTime(() -> now), is(nullValue()));
        }

        {
            // should always be gte 0
            ExplainIndexDataStreamLifecycle indexDataLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                true,
                now,
                now + 80L, // rolled over in the future (clocks are funny that way)
                TimeValue.timeValueMillis(now + 100L),
                new DataStreamLifecycle(),
                null
            );
            assertThat(indexDataLifecycle.getGenerationTime(() -> now), is(TimeValue.ZERO));
        }
    }

    public void testGetTimeSinceIndexCreation() {
        long now = System.currentTimeMillis();
        {
            ExplainIndexDataStreamLifecycle randomIndexDLMExplanation = createManagedIndexDLMExplanation(now, new DataStreamLifecycle());
            assertThat(randomIndexDLMExplanation.getTimeSinceIndexCreation(() -> now + 75L), is(TimeValue.timeValueMillis(75)));
        }
        {
            // null for unmanaged index
            ExplainIndexDataStreamLifecycle indexDataLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                false,
                null,
                null,
                null,
                null,
                null
            );
            assertThat(indexDataLifecycle.getTimeSinceIndexCreation(() -> now), is(nullValue()));
        }

        {
            // should always be gte 0
            ExplainIndexDataStreamLifecycle indexDataLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                true,
                now + 80L, // created in the future (clocks are funny that way)
                null,
                null,
                new DataStreamLifecycle(),
                null
            );
            assertThat(indexDataLifecycle.getTimeSinceIndexCreation(() -> now), is(TimeValue.ZERO));
        }
    }

    public void testGetTimeSinceRollover() {
        long now = System.currentTimeMillis();
        {
            ExplainIndexDataStreamLifecycle randomIndexDLMExplanation = createManagedIndexDLMExplanation(now, new DataStreamLifecycle());
            if (randomIndexDLMExplanation.getRolloverDate() == null) {
                // age calculated since creation date
                assertThat(randomIndexDLMExplanation.getTimeSinceRollover(() -> now + 50L), is(nullValue()));
            } else {
                assertThat(
                    randomIndexDLMExplanation.getTimeSinceRollover(() -> randomIndexDLMExplanation.getRolloverDate() + 75L),
                    is(TimeValue.timeValueMillis(75))
                );
            }
        }
        {
            // null for unmanaged index
            ExplainIndexDataStreamLifecycle indexDataLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                false,
                null,
                null,
                null,
                null,
                null
            );
            assertThat(indexDataLifecycle.getTimeSinceRollover(() -> now), is(nullValue()));
        }

        {
            // should always be gte 0
            ExplainIndexDataStreamLifecycle indexDataLifecycle = new ExplainIndexDataStreamLifecycle(
                "my-index",
                true,
                now - 50L,
                now + 100L, // rolled over in the future
                TimeValue.timeValueMillis(now),
                new DataStreamLifecycle(),
                null
            );
            assertThat(indexDataLifecycle.getTimeSinceRollover(() -> now), is(TimeValue.ZERO));
        }
    }

    @Override
    protected Writeable.Reader<ExplainIndexDataStreamLifecycle> instanceReader() {
        return ExplainIndexDataStreamLifecycle::new;
    }

    @Override
    protected ExplainIndexDataStreamLifecycle createTestInstance() {
        return createManagedIndexDLMExplanation(System.nanoTime(), randomBoolean() ? new DataStreamLifecycle() : null);
    }

    @Override
    protected ExplainIndexDataStreamLifecycle mutateInstance(ExplainIndexDataStreamLifecycle instance) throws IOException {
        return createManagedIndexDLMExplanation(System.nanoTime(), randomBoolean() ? new DataStreamLifecycle() : null);
    }

    private static ExplainIndexDataStreamLifecycle createManagedIndexDLMExplanation(long now, @Nullable DataStreamLifecycle lifecycle) {
        return new ExplainIndexDataStreamLifecycle(
            randomAlphaOfLengthBetween(10, 30),
            true,
            now,
            randomBoolean() ? now + TimeValue.timeValueDays(1).getMillis() : null,
            TimeValue.timeValueMillis(now),
            lifecycle,
            randomBoolean() ? new NullPointerException("bad times").getMessage() : null
        );
    }

}
