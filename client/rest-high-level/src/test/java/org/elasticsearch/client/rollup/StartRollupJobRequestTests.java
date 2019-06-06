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
package org.elasticsearch.client.rollup;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

public class StartRollupJobRequestTests extends ESTestCase {

    public void testConstructor() {
        String jobId = randomAlphaOfLength(5);
        assertEquals(jobId, new StartRollupJobRequest(jobId).getJobId());
    }

    public void testEqualsAndHash() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(new StartRollupJobRequest(randomAlphaOfLength(5)),
                orig -> new StartRollupJobRequest(orig.getJobId()),
                orig -> new StartRollupJobRequest(orig.getJobId() + "_suffix"));
    }

    public void testRequireJobId() {
        final NullPointerException e = expectThrows(NullPointerException.class, ()-> new StartRollupJobRequest(null));
        assertEquals("id parameter must not be null", e.getMessage());
    }

}
