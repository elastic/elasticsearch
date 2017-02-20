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

package org.elasticsearch.search.aggregations.metrics.geobounds;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.InternalAggregationTestCase;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;

public class InternalGeoBoundsTests extends InternalAggregationTestCase<InternalGeoBounds> {
    static final double GEOHASH_TOLERANCE = 1E-5D;

    @Override
    protected InternalGeoBounds createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
                                                   Map<String, Object> metaData) {
        InternalGeoBounds geo = new InternalGeoBounds(name,
            randomDouble(), randomDouble(), randomDouble(), randomDouble(),
            randomDouble(), randomDouble(), randomBoolean(),
            pipelineAggregators, Collections.emptyMap());
        return geo;
    }

    @Override
    protected void assertReduced(InternalGeoBounds reduced, List<InternalGeoBounds> inputs) {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double posLeft = Double.POSITIVE_INFINITY;
        double posRight = Double.NEGATIVE_INFINITY;
        double negLeft = Double.POSITIVE_INFINITY;
        double negRight = Double.NEGATIVE_INFINITY;
        for (InternalGeoBounds bounds : inputs) {
            if (bounds.top > top) {
                top = bounds.top;
            }
            if (bounds.bottom < bottom) {
                bottom = bounds.bottom;
            }
            if (bounds.posLeft < posLeft) {
                posLeft = bounds.posLeft;
            }
            if (bounds.posRight > posRight) {
                posRight = bounds.posRight;
            }
            if (bounds.negLeft < negLeft) {
                negLeft = bounds.negLeft;
            }
            if (bounds.negRight > negRight) {
                negRight = bounds.negRight;
            }
        }
        assertThat(reduced.top, closeTo(top, GEOHASH_TOLERANCE));
        assertThat(reduced.bottom, closeTo(bottom, GEOHASH_TOLERANCE));
        assertThat(reduced.posLeft, closeTo(posLeft, GEOHASH_TOLERANCE));
        assertThat(reduced.posRight, closeTo(posRight, GEOHASH_TOLERANCE));
        assertThat(reduced.negLeft, closeTo(negLeft, GEOHASH_TOLERANCE));
        assertThat(reduced.negRight, closeTo(negRight, GEOHASH_TOLERANCE));
    }

    @Override
    protected Writeable.Reader<InternalGeoBounds> instanceReader() {
        return InternalGeoBounds::new;
    }
}
