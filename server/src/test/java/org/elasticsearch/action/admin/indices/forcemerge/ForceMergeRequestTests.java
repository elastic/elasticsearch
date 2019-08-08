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
package org.elasticsearch.action.admin.indices.forcemerge;

import org.elasticsearch.test.ESTestCase;

public class ForceMergeRequestTests extends ESTestCase {

    public void testDescription() {
        ForceMergeRequest request = new ForceMergeRequest();
        assertEquals("Force-merge indices [], maxSegments[-1], onlyExpungeDeletes[false], flush[true]", request.getDescription());

        request = new ForceMergeRequest("shop", "blog");
        assertEquals("Force-merge indices [shop, blog], maxSegments[-1], onlyExpungeDeletes[false], flush[true]", request.getDescription());

        request = new ForceMergeRequest();
        request.maxNumSegments(12);
        request.onlyExpungeDeletes(true);
        request.flush(false);
        assertEquals("Force-merge indices [], maxSegments[12], onlyExpungeDeletes[true], flush[false]", request.getDescription());
    }
}
