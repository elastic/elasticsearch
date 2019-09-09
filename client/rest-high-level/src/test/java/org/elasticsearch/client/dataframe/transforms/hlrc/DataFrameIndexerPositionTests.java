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

package org.elasticsearch.client.dataframe.transforms.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.dataframe.transforms.DataFrameIndexerPosition;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class DataFrameIndexerPositionTests extends AbstractResponseTestCase<
        DataFrameIndexerPosition,
        org.elasticsearch.client.dataframe.transforms.DataFrameIndexerPosition> {

    public static DataFrameIndexerPosition fromHlrc(
            org.elasticsearch.client.dataframe.transforms.DataFrameIndexerPosition instance) {
        if (instance == null) {
            return null;
        }
        return new DataFrameIndexerPosition(instance.getIndexerPosition(), instance.getBucketsPosition());
    }

    public static DataFrameIndexerPosition randomDataFrameIndexerPosition() {
        return new DataFrameIndexerPosition(randomPositionMap(), randomPositionMap());
    }

    @Override
    protected DataFrameIndexerPosition createServerTestInstance(XContentType xContentType) {
        return randomDataFrameIndexerPosition();
    }

    @Override
    protected org.elasticsearch.client.dataframe.transforms.DataFrameIndexerPosition doParseToClientInstance(XContentParser parser) {
        return org.elasticsearch.client.dataframe.transforms.DataFrameIndexerPosition.fromXContent(parser);
    }

    @Override
    protected void assertInstances(DataFrameIndexerPosition serverTestInstance,
                                   org.elasticsearch.client.dataframe.transforms.DataFrameIndexerPosition clientInstance) {
        assertThat(serverTestInstance.getIndexerPosition(), equalTo(clientInstance.getIndexerPosition()));
        assertThat(serverTestInstance.getBucketsPosition(), equalTo(clientInstance.getBucketsPosition()));
    }

    private static Map<String, Object> randomPositionMap() {
        if (randomBoolean()) {
            return null;
        }
        int numFields = randomIntBetween(1, 5);
        Map<String, Object> position = new LinkedHashMap<>();
        for (int i = 0; i < numFields; i++) {
            Object value;
            if (randomBoolean()) {
                value = randomLong();
            } else {
                value = randomAlphaOfLengthBetween(1, 10);
            }
            position.put(randomAlphaOfLengthBetween(3, 10), value);
        }
        return position;
    }
}
