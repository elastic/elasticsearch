/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.dlm;

import org.elasticsearch.cluster.metadata.DataLifecycle;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ExplainIndexDataLifecycleTests extends AbstractWireSerializingTestCase<ExplainIndexDataLifecycle> {

    public void testGetGenerationTime() {
        long now = System.currentTimeMillis();
        {
            ExplainIndexDataLifecycle explainIndexDataLifecycle = new ExplainIndexDataLifecycle(
                randomAlphaOfLengthBetween(10, 30),
                true,
                now,
                randomBoolean() ? now + TimeValue.timeValueDays(1).getMillis() : null,
                null,
                new DataLifecycle(),
                randomBoolean() ? new NullPointerException("bad times").getMessage() : null
            );
            assertThat(explainIndexDataLifecycle.getGenerationTime(() -> now + 50L), is(nullValue()));
            explainIndexDataLifecycle = new ExplainIndexDataLifecycle(
                randomAlphaOfLengthBetween(10, 30),
                true,
                now,
                randomBoolean() ? now + TimeValue.timeValueDays(1).getMillis() : null,
                TimeValue.timeValueMillis(now + 100),
                new DataLifecycle(),
                randomBoolean() ? new NullPointerException("bad times").getMessage() : null
            );
            assertThat(explainIndexDataLifecycle.getGenerationTime(() -> now + 500L), is(TimeValue.timeValueMillis(400)));
        }
        {
            // null for unmanaged index
            ExplainIndexDataLifecycle indexDataLifecycle = new ExplainIndexDataLifecycle("my-index", false, null, null, null, null, null);
            assertThat(indexDataLifecycle.getGenerationTime(() -> now), is(nullValue()));
        }

        {
            // should always be gte 0
            ExplainIndexDataLifecycle indexDataLifecycle = new ExplainIndexDataLifecycle(
                "my-index",
                true,
                now,
                now + 80L, // rolled over in the future (clocks are funny that way)
                TimeValue.timeValueMillis(now + 100L),
                new DataLifecycle(),
                null
            );
            assertThat(indexDataLifecycle.getGenerationTime(() -> now), is(TimeValue.ZERO));
        }
    }

    public void testGetTimeSinceIndexCreation() {
        long now = System.currentTimeMillis();
        {
            ExplainIndexDataLifecycle randomIndexDLMExplanation = createManagedIndexDLMExplanation(now, new DataLifecycle());
            assertThat(randomIndexDLMExplanation.getTimeSinceIndexCreation(() -> now + 75L), is(TimeValue.timeValueMillis(75)));
        }
        {
            // null for unmanaged index
            ExplainIndexDataLifecycle indexDataLifecycle = new ExplainIndexDataLifecycle("my-index", false, null, null, null, null, null);
            assertThat(indexDataLifecycle.getTimeSinceIndexCreation(() -> now), is(nullValue()));
        }

        {
            // should always be gte 0
            ExplainIndexDataLifecycle indexDataLifecycle = new ExplainIndexDataLifecycle(
                "my-index",
                true,
                now + 80L, // created in the future (clocks are funny that way)
                null,
                null,
                new DataLifecycle(),
                null
            );
            assertThat(indexDataLifecycle.getTimeSinceIndexCreation(() -> now), is(TimeValue.ZERO));
        }
    }

    public void testGetTimeSinceRollover() {
        long now = System.currentTimeMillis();
        {
            ExplainIndexDataLifecycle randomIndexDLMExplanation = createManagedIndexDLMExplanation(now, new DataLifecycle());
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
            ExplainIndexDataLifecycle indexDataLifecycle = new ExplainIndexDataLifecycle("my-index", false, null, null, null, null, null);
            assertThat(indexDataLifecycle.getTimeSinceRollover(() -> now), is(nullValue()));
        }

        {
            // should always be gte 0
            ExplainIndexDataLifecycle indexDataLifecycle = new ExplainIndexDataLifecycle(
                "my-index",
                true,
                now - 50L,
                now + 100L, // rolled over in the future
                TimeValue.timeValueMillis(now),
                new DataLifecycle(),
                null
            );
            assertThat(indexDataLifecycle.getTimeSinceRollover(() -> now), is(TimeValue.ZERO));
        }
    }

    @Override
    protected Writeable.Reader<ExplainIndexDataLifecycle> instanceReader() {
        return ExplainIndexDataLifecycle::new;
    }

    @Override
    protected ExplainIndexDataLifecycle createTestInstance() {
        return createManagedIndexDLMExplanation(System.nanoTime(), randomBoolean() ? new DataLifecycle() : null);
    }

    @Override
    protected ExplainIndexDataLifecycle mutateInstance(ExplainIndexDataLifecycle instance) throws IOException {
        return createManagedIndexDLMExplanation(System.nanoTime(), randomBoolean() ? new DataLifecycle() : null);
    }

    private static ExplainIndexDataLifecycle createManagedIndexDLMExplanation(long now, @Nullable DataLifecycle lifecycle) {
        return new ExplainIndexDataLifecycle(
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
