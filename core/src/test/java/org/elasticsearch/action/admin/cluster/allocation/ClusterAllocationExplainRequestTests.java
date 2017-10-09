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

package org.elasticsearch.action.admin.cluster.allocation;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

public class ClusterAllocationExplainRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        ClusterAllocationExplainRequest request =
                new ClusterAllocationExplainRequest(randomAlphaOfLength(4), randomIntBetween(0, Integer.MAX_VALUE), randomBoolean(),
                                                       randomBoolean() ? randomAlphaOfLength(5) : null);
        request.includeYesDecisions(randomBoolean());
        request.includeDiskInfo(randomBoolean());
        BytesStreamOutput output = new BytesStreamOutput();
        request.writeTo(output);

        ClusterAllocationExplainRequest actual = new ClusterAllocationExplainRequest(output.bytes().streamInput());
        assertEquals(request.getIndex(), actual.getIndex());
        assertEquals(request.getShard(), actual.getShard());
        assertEquals(request.isPrimary(), actual.isPrimary());
        assertEquals(request.includeYesDecisions(), actual.includeYesDecisions());
        assertEquals(request.includeDiskInfo(), actual.includeDiskInfo());
        assertEquals(request.getCurrentNode(), actual.getCurrentNode());
    }

}
