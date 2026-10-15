/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.segments;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.codec.vectors.diskbbq.QuantEncoding;
import org.elasticsearch.index.codec.vectors.diskbbq.SegmentCalibrationParameters;
import org.elasticsearch.index.engine.Segment;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.ToXContent.EMPTY_PARAMS;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class IndicesSegmentResponseTests extends ESTestCase {

    public void testToXContentSerialiationWithSortedFields() throws Exception {
        ShardRouting shardRouting = TestShardRouting.newShardRouting("foo", 0, "node_id", true, ShardRoutingState.STARTED);
        Segment segment = new Segment("my");

        SortField sortField = new SortField("foo", SortField.Type.STRING, false, SortField.STRING_LAST);
        segment.segmentSort = new Sort(sortField);

        ShardSegments shardSegments = new ShardSegments(shardRouting, Collections.singletonList(segment));
        IndicesSegmentResponse response = new IndicesSegmentResponse(
            new ShardSegments[] { shardSegments },
            1,
            1,
            0,
            Collections.emptyList()
        );
        try (XContentBuilder builder = jsonBuilder()) {
            ChunkedToXContent.wrapAsToXContent(response).toXContent(builder, EMPTY_PARAMS);
        }
    }

    public void testToXContentWithCalibratedField() throws Exception {
        ShardRouting shardRouting = TestShardRouting.newShardRouting("foo", 0, "node_id", true, ShardRoutingState.STARTED);
        Segment segment = new Segment("_0");
        segment.autoCalibrationParams = Map.of(
            "my_field",
            new SegmentCalibrationParameters.Osq(QuantEncoding.FOUR_BIT_SYMMETRIC, true, 3.0f)
        );
        segment.autoCalibrationVectorCounts = Map.of("my_field", 12345L);
        segment.autoCalibrationSizeBytes = Map.of("my_field", 8192L);

        ShardSegments shardSegments = new ShardSegments(shardRouting, Collections.singletonList(segment));
        IndicesSegmentResponse response = new IndicesSegmentResponse(
            new ShardSegments[] { shardSegments },
            1,
            1,
            0,
            Collections.emptyList()
        );

        String json = XContentHelper.toXContent(response, XContentType.JSON, EMPTY_PARAMS, false).utf8ToString();
        assertThat(json, org.hamcrest.Matchers.containsString("\"auto_calibration\""));
        assertThat(json, org.hamcrest.Matchers.containsString("\"my_field\""));
        assertThat(json, org.hamcrest.Matchers.containsString("\"calibrated\":true"));
        assertThat(json, org.hamcrest.Matchers.containsString("\"type\":\"osq\""));
        assertThat(json, org.hamcrest.Matchers.containsString("\"number_of_vectors\":12345"));
        assertThat(json, org.hamcrest.Matchers.containsString("\"size_in_bytes\":8192"));
        assertThat(json, org.hamcrest.Matchers.containsString("\"parameters\""));
        assertThat(json, org.hamcrest.Matchers.containsString("\"bits\":4"));
        assertThat(json, org.hamcrest.Matchers.containsString("\"query_bits\":4"));
        assertThat(json, org.hamcrest.Matchers.containsString("\"precondition\":true"));
        assertThat(json, org.hamcrest.Matchers.containsString("\"oversample\":3.0"));

        ShardSegments withoutCalibration = new ShardSegments(shardRouting, Collections.singletonList(new Segment("_1")));
        IndicesSegmentResponse responseWithoutCalibration = new IndicesSegmentResponse(
            new ShardSegments[] { withoutCalibration },
            1,
            1,
            0,
            Collections.emptyList()
        );
        String jsonWithoutCalibration = XContentHelper.toXContent(responseWithoutCalibration, XContentType.JSON, EMPTY_PARAMS, false)
            .utf8ToString();
        assertThat(jsonWithoutCalibration, org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("auto_calibration")));
    }

    public void testToXContentWithUncalibratedField() throws Exception {
        ShardRouting shardRouting = TestShardRouting.newShardRouting("foo", 0, "node_id", true, ShardRoutingState.STARTED);
        Segment segment = new Segment("_0");
        segment.autoCalibrationParams = Map.of("my_field", new SegmentCalibrationParameters.Osq(null, false, Float.NaN));

        ShardSegments shardSegments = new ShardSegments(shardRouting, Collections.singletonList(segment));
        IndicesSegmentResponse response = new IndicesSegmentResponse(
            new ShardSegments[] { shardSegments },
            1,
            1,
            0,
            Collections.emptyList()
        );

        String json = XContentHelper.toXContent(response, XContentType.JSON, EMPTY_PARAMS, false).utf8ToString();
        assertThat(json, org.hamcrest.Matchers.containsString("\"auto_calibration\""));
        assertThat(json, org.hamcrest.Matchers.containsString("\"my_field\""));
        assertThat(json, org.hamcrest.Matchers.containsString("\"calibrated\":false"));
        assertThat(json, org.hamcrest.Matchers.containsString("\"type\":\"osq\""));
        assertThat(json, org.hamcrest.Matchers.containsString("\"number_of_vectors\":0"));
        assertThat(json, org.hamcrest.Matchers.containsString("\"size_in_bytes\":0"));
        assertThat(json, org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("\"parameters\"")));
        assertThat(json, org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("\"bits\"")));
        assertThat(json, org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("\"oversample\"")));
    }

    public void testChunking() {
        final int indices = randomIntBetween(1, 10);
        final List<ShardRouting> routings = new ArrayList<>(indices);
        for (int i = 0; i < indices; i++) {
            routings.add(TestShardRouting.newShardRouting("index-" + i, 0, "node_id", true, ShardRoutingState.STARTED));
        }
        Segment segment = new Segment("my");
        SortField sortField = new SortField("foo", SortField.Type.STRING, false, SortField.STRING_LAST);
        segment.segmentSort = new Sort(sortField);
        AbstractChunkedSerializingTestCase.assertChunkCount(
            new IndicesSegmentResponse(
                routings.stream().map(routing -> new ShardSegments(routing, List.of(segment))).toArray(ShardSegments[]::new),
                indices,
                indices,
                0,
                Collections.emptyList()
            ),
            response -> 11 * response.getIndices().size() + 4
        );
    }
}
