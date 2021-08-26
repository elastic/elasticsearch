/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.latest;

import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class LatestChangeCollectorTests extends ESTestCase {

    public void testBuildFilterQuery() {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp");

        assertThat(
            changeCollector.buildFilterQuery(
                new TransformCheckpoint("t_id", 42L, 42L, Collections.emptyMap(), 0L),
                new TransformCheckpoint("t_id", 42L, 42L, Collections.emptyMap(), 123456789L)
            ),
            is(equalTo(QueryBuilders.rangeQuery("timestamp").gte(0L).lt(123456789L).format("epoch_millis")))
        );

        assertThat(
            changeCollector.buildFilterQuery(
                new TransformCheckpoint("t_id", 42L, 42L, Collections.emptyMap(), 123456789L),
                new TransformCheckpoint("t_id", 42L, 42L, Collections.emptyMap(), 234567890L)
            ),
            is(equalTo(QueryBuilders.rangeQuery("timestamp").gte(123456789L).lt(234567890L).format("epoch_millis")))
        );
    }

    public void testGetIndicesToQuery() {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp");

        long[] indexSequenceIds1 = { 25L, 25L, 25L };
        long[] indexSequenceIds2 = { 324L, 2425L, 2225L };
        long[] indexSequenceIds3 = { 244L, 225L, 2425L };
        long[] indexSequenceIds4 = { 2005L, 2445L, 2425L };

        long[] indexSequenceIds3_1 = { 246L, 255L, 2485L };
        long[] indexSequenceIds4_1 = { 2105L, 2545L, 2525L };

        // no changes
        assertThat(
            changeCollector.getIndicesToQuery(
                new TransformCheckpoint(
                    "t_id",
                    123513L,
                    42L,
                    org.elasticsearch.core.Map.of(
                        "index-1",
                        indexSequenceIds1,
                        "index-2",
                        indexSequenceIds2,
                        "index-3",
                        indexSequenceIds3,
                        "index-4",
                        indexSequenceIds4
                    ),
                    123543L
                ),
                new TransformCheckpoint(
                    "t_id",
                    123456759L,
                    43L,
                    org.elasticsearch.core.Map.of(
                        "index-1",
                        indexSequenceIds1,
                        "index-2",
                        indexSequenceIds2,
                        "index-3",
                        indexSequenceIds3,
                        "index-4",
                        indexSequenceIds4
                    ),
                    123456789L
                )
            ),
            equalTo(Collections.emptySet())
        );

        // 3 and 4 changed, 1 and 2 not
        assertThat(
            changeCollector.getIndicesToQuery(
                new TransformCheckpoint(
                    "t_id",
                    123513L,
                    42L,
                    org.elasticsearch.core.Map.of(
                        "index-1",
                        indexSequenceIds1,
                        "index-2",
                        indexSequenceIds2,
                        "index-3",
                        indexSequenceIds3,
                        "index-4",
                        indexSequenceIds4
                    ),
                    123543L
                ),
                new TransformCheckpoint(
                    "t_id",
                    123456759L,
                    43L,
                    org.elasticsearch.core.Map.of(
                        "index-1",
                        indexSequenceIds1,
                        "index-2",
                        indexSequenceIds2,
                        "index-3",
                        indexSequenceIds3_1,
                        "index-4",
                        indexSequenceIds4_1
                    ),
                    123456789L
                )
            ),
            equalTo(org.elasticsearch.core.Set.of("index-3", "index-4"))
        );

        // only 3 changed (no order)
        assertThat(
            changeCollector.getIndicesToQuery(
                new TransformCheckpoint(
                    "t_id",
                    123513L,
                    42L,
                    org.elasticsearch.core.Map.of(
                        "index-1",
                        indexSequenceIds1,
                        "index-2",
                        indexSequenceIds2,
                        "index-3",
                        indexSequenceIds3,
                        "index-4",
                        indexSequenceIds4
                    ),
                    123543L
                ),
                new TransformCheckpoint(
                    "t_id",
                    123456759L,
                    43L,
                    org.elasticsearch.core.Map.of(
                        "index-1",
                        indexSequenceIds1,
                        "index-2",
                        indexSequenceIds2,
                        "index-3",
                        indexSequenceIds3_1,
                        "index-4",
                        indexSequenceIds4
                    ),
                    123456789L
                )
            ),
            equalTo(Collections.singleton("index-3"))
        );

        // all have changed
        assertThat(
            changeCollector.getIndicesToQuery(
                new TransformCheckpoint(
                    "t_id",
                    123513L,
                    42L,
                    org.elasticsearch.core.Map.of("index-3", indexSequenceIds3, "index-4", indexSequenceIds4),
                    123543L
                ),
                new TransformCheckpoint(
                    "t_id",
                    123456759L,
                    43L,
                    org.elasticsearch.core.Map.of("index-3", indexSequenceIds3_1, "index-4", indexSequenceIds4_1),
                    123456789L
                )
            ),
            equalTo(org.elasticsearch.core.Set.of("index-3", "index-4"))
        );

        // a new index appeared
        assertThat(
            changeCollector.getIndicesToQuery(
                new TransformCheckpoint(
                    "t_id",
                    123513L,
                    42L,
                    org.elasticsearch.core.Map.of("index-2", indexSequenceIds2, "index-3", indexSequenceIds3, "index-4", indexSequenceIds4),
                    123543L
                ),
                new TransformCheckpoint(
                    "t_id",
                    123456759L,
                    43L,
                    org.elasticsearch.core.Map.of(
                        "index-1",
                        indexSequenceIds1,
                        "index-2",
                        indexSequenceIds2,
                        "index-3",
                        indexSequenceIds3_1,
                        "index-4",
                        indexSequenceIds4_1
                    ),
                    123456789L
                )
            ),
            equalTo(org.elasticsearch.core.Set.of("index-1", "index-3", "index-4"))
        );

        // index disappeared
        assertThat(
            changeCollector.getIndicesToQuery(
                new TransformCheckpoint(
                    "t_id",
                    123513L,
                    42L,
                    org.elasticsearch.core.Map.of(
                        "index-1",
                        indexSequenceIds1,
                        "index-2",
                        indexSequenceIds2,
                        "index-3",
                        indexSequenceIds3,
                        "index-4",
                        indexSequenceIds4
                    ),
                    123543L
                ),
                new TransformCheckpoint(
                    "t_id",
                    123456759L,
                    43L,
                    org.elasticsearch.core.Map.of(
                        "index-2",
                        indexSequenceIds2,
                        "index-3",
                        indexSequenceIds3_1,
                        "index-4",
                        indexSequenceIds4_1
                    ),
                    123456789L
                )
            ),
            equalTo(org.elasticsearch.core.Set.of("index-3", "index-4"))
        );
    }
}
