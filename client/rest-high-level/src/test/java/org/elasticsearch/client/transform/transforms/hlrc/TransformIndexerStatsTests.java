/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
