/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client.ml.dataframe;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class OutlierDetectionTests extends AbstractXContentTestCase<OutlierDetection> {

    public static OutlierDetection randomOutlierDetection() {
        return OutlierDetection.builder()
            .setNNeighbors(randomBoolean() ? null : randomIntBetween(1, 20))
            .setMethod(randomBoolean() ? null : randomFrom(OutlierDetection.Method.values()))
            .setFeatureInfluenceThreshold(randomBoolean() ? null : randomDoubleBetween(0.0, 1.0, true))
            .build();
    }

    @Override
    protected OutlierDetection doParseInstance(XContentParser parser) throws IOException {
        return OutlierDetection.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected OutlierDetection createTestInstance() {
        return randomOutlierDetection();
    }

    public void testGetParams_GivenDefaults() {
        OutlierDetection outlierDetection = OutlierDetection.createDefault();
        assertNull(outlierDetection.getNNeighbors());
        assertNull(outlierDetection.getMethod());
        assertNull(outlierDetection.getFeatureInfluenceThreshold());
    }

    public void testGetParams_GivenExplicitValues() {
        OutlierDetection outlierDetection =
            OutlierDetection.builder()
                .setNNeighbors(42)
                .setMethod(OutlierDetection.Method.LDOF)
                .setFeatureInfluenceThreshold(0.5)
                .build();
        assertThat(outlierDetection.getNNeighbors(), equalTo(42));
        assertThat(outlierDetection.getMethod(), equalTo(OutlierDetection.Method.LDOF));
        assertThat(outlierDetection.getFeatureInfluenceThreshold(), closeTo(0.5, 1E-9));
    }
}
