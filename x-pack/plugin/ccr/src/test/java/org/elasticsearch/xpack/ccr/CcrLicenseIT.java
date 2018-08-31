/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.ccr.action.CreateAndFollowIndexAction;
import org.elasticsearch.xpack.ccr.action.FollowIndexAction;
import org.elasticsearch.xpack.ccr.action.ShardFollowNodeTask;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CcrLicenseIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(IncompatibleLicenseLocalStateCcr.class);
    }

    public void testThatFollowingIndexIsUnavailableWithIncompatibleLicense() throws InterruptedException {
        final FollowIndexAction.Request followRequest = getFollowRequest();
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(
                FollowIndexAction.INSTANCE,
                followRequest,
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(final AcknowledgedResponse response) {
                        fail();
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        assertIncompatibleLicense(e);
                        latch.countDown();
                    }
                });
        latch.await();
    }

    public void testThatCreateAndFollowingIndexIsUnavailableWithIncompatibleLicense() throws InterruptedException {
        final FollowIndexAction.Request followRequest = getFollowRequest();
        final CreateAndFollowIndexAction.Request createAndFollowRequest = new CreateAndFollowIndexAction.Request(followRequest);
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(
                CreateAndFollowIndexAction.INSTANCE,
                createAndFollowRequest,
                new ActionListener<CreateAndFollowIndexAction.Response>() {
                    @Override
                    public void onResponse(final CreateAndFollowIndexAction.Response response) {
                        fail();
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        assertIncompatibleLicense(e);
                        latch.countDown();
                    }
                });
        latch.await();
    }

    public void testThatCcrStatsAreUnavailableWithIncompatibleLicense() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(CcrStatsAction.INSTANCE, new CcrStatsAction.TasksRequest(), new ActionListener<CcrStatsAction.TasksResponse>() {
            @Override
            public void onResponse(final CcrStatsAction.TasksResponse tasksResponse) {
                fail();
            }

            @Override
            public void onFailure(final Exception e) {
                assertIncompatibleLicense(e);
                latch.countDown();
            }
        });

        latch.await();
    }

    private void assertIncompatibleLicense(final Exception e) {
        assertThat(e, instanceOf(ElasticsearchSecurityException.class));
        assertThat(e.getMessage(), equalTo("current license is non-compliant for [ccr]"));
    }

    private FollowIndexAction.Request getFollowRequest() {
        return new FollowIndexAction.Request(
                "leader",
                "follower",
                ShardFollowNodeTask.DEFAULT_MAX_BATCH_OPERATION_COUNT,
                ShardFollowNodeTask.DEFAULT_MAX_CONCURRENT_READ_BATCHES,
                ShardFollowNodeTask.DEFAULT_MAX_BATCH_SIZE_IN_BYTES,
                ShardFollowNodeTask.DEFAULT_MAX_CONCURRENT_WRITE_BATCHES,
                ShardFollowNodeTask.DEFAULT_MAX_WRITE_BUFFER_SIZE,
                TimeValue.timeValueMillis(10),
                TimeValue.timeValueMillis(10));
    }

}
