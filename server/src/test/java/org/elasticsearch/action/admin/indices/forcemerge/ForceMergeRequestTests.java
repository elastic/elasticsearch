/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
        final int shardNumber = randomIntBetween(ForceMergeRequest.Defaults.SHARD_NUMBER, 100);

        final ForceMergeRequest request = new ForceMergeRequest();
        request.flush(flush);
        request.onlyExpungeDeletes(onlyExpungeDeletes);
        request.maxNumSegments(maxNumSegments);
        request.shardNumber(shardNumber);

        assertThat(request.flush(), equalTo(flush));
        assertThat(request.onlyExpungeDeletes(), equalTo(onlyExpungeDeletes));
        assertThat(request.maxNumSegments(), equalTo(maxNumSegments));
        assertThat(request.shardNumber(), equalTo(shardNumber));

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
        assertEquals("Force-merge indices [], maxSegments[-1], shardNumber[-1], onlyExpungeDeletes[false], flush[true]", request.getDescription());

        request = new ForceMergeRequest("shop", "blog");
        assertEquals("Force-merge indices [shop, blog], maxSegments[-1], shardNumber[-1], onlyExpungeDeletes[false], flush[true]", request.getDescription());

        request = new ForceMergeRequest();
        request.maxNumSegments(12);
        request.shardNumber(12);
        request.onlyExpungeDeletes(true);
        request.flush(false);
        assertEquals("Force-merge indices [], maxSegments[12], shardNumber[12], onlyExpungeDeletes[true], flush[false]", request.getDescription());
    }
}
