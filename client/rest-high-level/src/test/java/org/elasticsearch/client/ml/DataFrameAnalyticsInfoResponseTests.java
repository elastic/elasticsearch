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
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.dataframe.info.FieldSelection;
import org.elasticsearch.client.ml.dataframe.info.FieldSelectionTests;
import org.elasticsearch.client.ml.dataframe.info.MemoryEstimation;
import org.elasticsearch.client.ml.dataframe.info.MemoryEstimationTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

public class DataFrameAnalyticsInfoResponseTests extends AbstractXContentTestCase<DataFrameAnalyticsInfoResponse> {

    @Override
    protected DataFrameAnalyticsInfoResponse createTestInstance() {
        int fieldSelectionCount = randomIntBetween(1, 5);
        List<FieldSelection> fieldSelection = new ArrayList<>(fieldSelectionCount);
        IntStream.of(fieldSelectionCount).forEach(i -> fieldSelection.add(FieldSelectionTests.createRandom()));
        MemoryEstimation memoryEstimation = MemoryEstimationTests.createRandom();

        return new DataFrameAnalyticsInfoResponse(fieldSelection, memoryEstimation);
    }

    @Override
    protected DataFrameAnalyticsInfoResponse doParseInstance(XContentParser parser) throws IOException {
        return DataFrameAnalyticsInfoResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
