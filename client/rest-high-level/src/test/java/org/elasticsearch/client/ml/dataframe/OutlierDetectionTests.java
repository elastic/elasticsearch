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
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class OutlierDetectionTests extends AbstractXContentTestCase<OutlierDetection> {

    public static OutlierDetection randomOutlierDetection() {
        return OutlierDetection.builder()
            .setNNeighbors(randomBoolean() ? null : randomIntBetween(1, 20))
            .setMethod(randomBoolean() ? null : randomFrom(OutlierDetection.Method.values()))
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
        OutlierDetection outlierDetection = OutlierDetection.builder().build();
        assertThat(outlierDetection.getParams().isEmpty(), is(true));
    }

    public void testGetParams_GivenExplicitValues() {
        OutlierDetection outlierDetection =
            OutlierDetection.builder()
                .setNNeighbors(42)
                .setMethod(OutlierDetection.Method.LDOF)
                .build();

        Map<String, Object> params = outlierDetection.getParams();

        assertThat(params.size(), equalTo(2));
        assertThat(params.get(OutlierDetection.N_NEIGHBORS.getPreferredName()), equalTo(42));
        assertThat(params.get(OutlierDetection.METHOD.getPreferredName()), equalTo(OutlierDetection.Method.LDOF));
    }
}
