/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.client.transform.transforms.TransformIndexerStats;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.client.transform.transforms.hlrc.TransformStatsTests.assertTransformIndexerStats;

public class TransformIndexerStatsTests extends AbstractResponseTestCase<
    org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats,
    TransformIndexerStats> {

    public static org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats randomStats() {
        return new org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats(
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomLongBetween(0L, 10000L),
            randomDouble(),
            randomDouble(),
            randomDouble()
        );
    }

    @Override
    protected org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats createServerTestInstance(XContentType xContentType) {
        return randomStats();
    }

    @Override
    protected TransformIndexerStats doParseToClientInstance(XContentParser parser) throws IOException {
        return TransformIndexerStats.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        org.elasticsearch.xpack.core.transform.transforms.TransformIndexerStats serverTestInstance,
        TransformIndexerStats clientInstance
    ) {
        assertTransformIndexerStats(serverTestInstance, clientInstance);
    }
}
