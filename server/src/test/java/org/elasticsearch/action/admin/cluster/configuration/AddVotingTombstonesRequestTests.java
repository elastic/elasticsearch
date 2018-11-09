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
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class AddVotingTombstonesRequestTests extends ESTestCase {
    public void testSerialization() throws IOException {
        int descriptionCount = between(0, 5);
        String[] descriptions = new String[descriptionCount];
        for (int i = 0; i < descriptionCount; i++) {
            descriptions[i] = randomAlphaOfLength(10);
        }
        TimeValue timeout = TimeValue.timeValueMillis(between(0, 30000));
        final AddVotingTombstonesRequest originalRequest = new AddVotingTombstonesRequest(descriptions, timeout);
        final AddVotingTombstonesRequest deserialized = copyWriteable(originalRequest, writableRegistry(), AddVotingTombstonesRequest::new);
        assertThat(deserialized.getNodeDescriptions(), equalTo(originalRequest.getNodeDescriptions()));
        assertThat(deserialized.getTimeout(), equalTo(originalRequest.getTimeout()));
    }
}
