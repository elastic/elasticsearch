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

public class GetRollupJobRequestTests extends ESTestCase {
    public void testRequiresJob() {
        final NullPointerException e = expectThrows(NullPointerException.class, () -> new GetRollupJobRequest(null));
        assertEquals("jobId is required", e.getMessage());
    }

    public void testDoNotUseAll() {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new GetRollupJobRequest("_all"));
        assertEquals("use the default ctor to ask for all jobs", e.getMessage());
    }
}
