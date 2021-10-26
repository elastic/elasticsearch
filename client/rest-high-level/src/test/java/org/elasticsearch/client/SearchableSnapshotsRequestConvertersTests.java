/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.client.searchable_snapshots.CachesStatsRequest;
import org.elasticsearch.client.searchable_snapshots.MountSnapshotRequest;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class SearchableSnapshotsRequestConvertersTests extends ESTestCase {

    public void testMountSnapshot() throws IOException  {
        final MountSnapshotRequest request =
            new MountSnapshotRequest(randomAlphaOfLength(8), randomAlphaOfLength(8), randomAlphaOfLength(8));
        if (randomBoolean()) {
            request.masterTimeout(TimeValue.parseTimeValue(randomTimeValue(), "master_timeout"));
        }
        if (randomBoolean()) {
            request.waitForCompletion(randomBoolean());
        }
        if (randomBoolean()) {
            request.storage(randomFrom(MountSnapshotRequest.Storage.FULL_COPY, MountSnapshotRequest.Storage.SHARED_CACHE));
        }
        final Request result = SearchableSnapshotsRequestConverters.mountSnapshot(request);
        assertThat(result.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(result.getEndpoint(), equalTo("/_snapshot/" + request.getRepository() + "/" + request.getSnapshot() + "/_mount"));
        if (request.getMasterTimeout() != null) {
            final TimeValue expectedValue = request.getMasterTimeout();
            assertThat(result.getParameters().get("master_timeout"), is(expectedValue.getStringRep()));
        } else {
            assertThat(result.getParameters().get("master_timeout"), nullValue());
        }
        if (request.getWaitForCompletion() != null) {
            assertThat(result.getParameters().get("wait_for_completion"), is(Boolean.toString(request.getWaitForCompletion())));
        } else {
            assertThat(result.getParameters().get("wait_for_completion"), nullValue());
        }
        if (request.getStorage() != null) {
            assertThat(result.getParameters().get("storage"), is(request.getStorage().storageName()));
        } else {
            assertThat(result.getParameters().get("storage"), nullValue());
        }
        RequestConvertersTests.assertToXContentBody(request, result.getEntity());
    }

    public void testCachesStats() throws IOException  {
        {
            final Request request = SearchableSnapshotsRequestConverters.cacheStats(new CachesStatsRequest());
            assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
            assertThat(request.getEndpoint(), equalTo("/_searchable_snapshots/cache/stats"));
        }
        {
            final String[] nodesIds = generateRandomStringArray(10, 5, false, false);
            final Request request = SearchableSnapshotsRequestConverters.cacheStats(new CachesStatsRequest(nodesIds));
            assertThat(request.getMethod(), equalTo(HttpGet.METHOD_NAME));
            assertThat(request.getEndpoint(), equalTo("/_searchable_snapshots/" + String.join(",", nodesIds) + "/cache/stats"));
        }
    }
}
