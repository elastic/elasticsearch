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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ForceMergeRequestTests extends ESTestCase {

    public void testValidate() {
        final boolean flush = randomBoolean();
        final boolean onlyExpungeDeletes = randomBoolean();
        final int maxNumSegments = randomIntBetween(ForceMergeRequest.Defaults.MAX_NUM_SEGMENTS, 100);

        final ForceMergeRequest request = new ForceMergeRequest();
        request.flush(flush);
        request.onlyExpungeDeletes(onlyExpungeDeletes);
        request.maxNumSegments(maxNumSegments);

        assertThat(request.flush(), equalTo(flush));
        assertThat(request.onlyExpungeDeletes(), equalTo(onlyExpungeDeletes));
        assertThat(request.maxNumSegments(), equalTo(maxNumSegments));

        ActionRequestValidationException validation = request.validate();
        if (onlyExpungeDeletes && maxNumSegments != ForceMergeRequest.Defaults.MAX_NUM_SEGMENTS) {
            assertThat(validation, notNullValue());
            assertThat(validation.validationErrors(), contains("cannot set only_expunge_deletes and max_num_segments at the "
                + "same time, those two parameters are mutually exclusive"));
        } else {
            assertThat(validation, nullValue());
        }
    }

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
