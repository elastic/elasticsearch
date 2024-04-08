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
import static org.hamcrest.Matchers.equalTo;

public class GetSnapshotsRequestTests extends ESTestCase {

    public void testValidateParameters() {
        {
            final GetSnapshotsRequest request = new GetSnapshotsRequest("repo", "snapshot");
            assertNull(request.validate());
            request.size(0);
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("size must be -1 or greater than 0"));
        }
        {
            final GetSnapshotsRequest request = new GetSnapshotsRequest("repo", "snapshot").size(randomIntBetween(1, 500));
            assertNull(request.validate());
        }
        {
            final GetSnapshotsRequest request = new GetSnapshotsRequest("repo", "snapshot").verbose(false).size(randomIntBetween(1, 500));
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("can't use size limit with verbose=false"));
        }
        {
            final GetSnapshotsRequest request = new GetSnapshotsRequest("repo", "snapshot").verbose(false).offset(randomIntBetween(1, 500));
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("can't use offset with verbose=false"));
        }
        {
            final GetSnapshotsRequest request = new GetSnapshotsRequest("repo", "snapshot").verbose(false).sort(SnapshotSortKey.INDICES);
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("can't use non-default sort with verbose=false"));
        }
        {
            final GetSnapshotsRequest request = new GetSnapshotsRequest("repo", "snapshot").verbose(false).order(SortOrder.DESC);
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("can't use non-default sort order with verbose=false"));
        }
        {
            final GetSnapshotsRequest request = new GetSnapshotsRequest("repo", "snapshot").verbose(false)
                .after(new SnapshotSortKey.After("foo", "repo", "bar"));
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("can't use after with verbose=false"));
        }
        {
            final GetSnapshotsRequest request = new GetSnapshotsRequest("repo", "snapshot").verbose(false).fromSortValue("bar");
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("can't use from_sort_value with verbose=false"));
        }
        {
            final GetSnapshotsRequest request = new GetSnapshotsRequest("repo", "snapshot").after(
                new SnapshotSortKey.After("foo", "repo", "bar")
            ).offset(randomIntBetween(1, 500));
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("can't use after and offset simultaneously"));
        }
        {
            final GetSnapshotsRequest request = new GetSnapshotsRequest("repo", "snapshot").fromSortValue("foo")
                .after(new SnapshotSortKey.After("foo", "repo", "bar"));
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("can't use after and from_sort_value simultaneously"));
        }
        {
            final GetSnapshotsRequest request = new GetSnapshotsRequest("repo", "snapshot").policies("some-policy").verbose(false);
            final ActionRequestValidationException e = request.validate();
            assertThat(e.getMessage(), containsString("can't use slm policy filter with verbose=false"));
        }
    }

    public void testGetDescription() {
        final GetSnapshotsRequest request = new GetSnapshotsRequest(
            new String[] { "repo1", "repo2" },
            new String[] { "snapshotA", "snapshotB" }
        );
        assertThat(request.getDescription(), equalTo("repositories[repo1,repo2], snapshots[snapshotA,snapshotB]"));
    }
}
