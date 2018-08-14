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
package org.elasticsearch.protocol.xpack.ml;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

public class CloseJobRequestTests extends ESTestCase {

    public void testCloseAllJobsRequest() {
        CloseJobRequest request = CloseJobRequest.closeAllJobsRequest();
        assertEquals(request.getJobIds().size(), 1);
        assertEquals(request.getJobIds().get(0), CloseJobRequest.ALL_JOBS);
    }

    public void testAddNullJobIds() {
        CloseJobRequest request = new CloseJobRequest();
        List<String> jobIds = Arrays.asList("job1", null);

        expectThrows(NullPointerException.class, () -> request.addJobId(null));
        expectThrows(NullPointerException.class, () -> request.setJobIds(null));
        expectThrows(NullPointerException.class, () -> request.setJobIds(jobIds));
    }

    public void testDefaultValues() {
        CloseJobRequest request = new CloseJobRequest();

        assertTrue(request.isAllowNoJobs());
        assertFalse(request.isForce());
        assertNull(request.getTimeout());
        assertTrue(request.getJobIds().isEmpty());
    }

    public void testGetCommaDelimitedJobIdString() {
        CloseJobRequest request = new CloseJobRequest("job1");
        assertEquals(request.getCommaDelimitedJobIdString(), "job1");

        request.addJobId("otherjobs*");
        assertEquals(request.getCommaDelimitedJobIdString(), "job1,otherjobs*");
    }
}
