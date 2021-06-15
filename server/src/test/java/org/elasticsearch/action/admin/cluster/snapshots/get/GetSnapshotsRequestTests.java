/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class GetSnapshotsRequestTests extends ESTestCase {

    public void testValidateParameters() {
        final GetSnapshotsRequest request = new GetSnapshotsRequest("repo", "snapshot");
        assertNull(request.validate());
        request.size(0);
        {
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("size must be -1 or greater than 0"));
        }
        request.size(randomIntBetween(1, 500));
        assertNull(request.validate());
        request.verbose(false);
        {
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("can't use size limit with verbose=false"));
        }
        request.sort(GetSnapshotsRequest.SortBy.INDICES);
        {
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("can't use non-default sort with verbose=false"));
        }
        request.order(SortOrder.DESC);
        {
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("can't use non-default sort order with verbose=false"));
        }
        request.after(new GetSnapshotsRequest.After("foo", "bar"));
        {
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("can't use after with verbose=false"));
        }
    }
}
