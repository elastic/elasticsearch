/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xpack.ccr.action.AutoFollowCoordinator;
import org.elasticsearch.xpack.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.ccr.action.CreateAndFollowIndexAction;
import org.elasticsearch.xpack.ccr.action.FollowIndexAction;
import org.elasticsearch.xpack.ccr.action.PutAutoFollowPatternAction;
import org.elasticsearch.xpack.ccr.action.ShardFollowNodeTask;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CcrLicenseIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(NonCompliantLicenseLocalStateCcr.class);
    }

    public void testThatFollowingIndexIsUnavailableWithNonCompliantLicense() throws InterruptedException {
        final FollowIndexAction.Request followRequest = getFollowRequest();
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(
                FollowIndexAction.INSTANCE,
                followRequest,
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(final AcknowledgedResponse response) {
                        latch.countDown();
                        fail();
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        assertNonCompliantLicense(e);
                        latch.countDown();
                    }
                });
        latch.await();
    }

    public void testThatCreateAndFollowingIndexIsUnavailableWithNonCompliantLicense() throws InterruptedException {
        final FollowIndexAction.Request followRequest = getFollowRequest();
        final CreateAndFollowIndexAction.Request createAndFollowRequest = new CreateAndFollowIndexAction.Request(followRequest);
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(
                CreateAndFollowIndexAction.INSTANCE,
                createAndFollowRequest,
                new ActionListener<CreateAndFollowIndexAction.Response>() {
                    @Override
                    public void onResponse(final CreateAndFollowIndexAction.Response response) {
                        latch.countDown();
                        fail();
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        assertNonCompliantLicense(e);
                        latch.countDown();
                    }
                });
        latch.await();
    }

    public void testThatCcrStatsAreUnavailableWithNonCompliantLicense() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(CcrStatsAction.INSTANCE, new CcrStatsAction.TasksRequest(), new ActionListener<CcrStatsAction.TasksResponse>() {
            @Override
            public void onResponse(final CcrStatsAction.TasksResponse tasksResponse) {
                latch.countDown();
                fail();
            }

            @Override
            public void onFailure(final Exception e) {
                assertNonCompliantLicense(e);
                latch.countDown();
            }
        });

        latch.await();
    }

    public void testThatPutAutoFollowPatternsIsUnavailableWithNonCompliantLicense() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setLeaderClusterAlias("leader");
        request.setLeaderIndexPatterns(Collections.singletonList("*"));
        client().execute(
                PutAutoFollowPatternAction.INSTANCE,
                request,
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(final AcknowledgedResponse response) {
                        latch.countDown();
                        fail();
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        assertNonCompliantLicense(e);
                        latch.countDown();
                    }
                });
        latch.await();
    }

    public void testAutoFollowCoordinatorLogsSkippingAutoFollowCoordinationWithNonCompliantLicense() throws Exception {
        final Logger logger = LogManager.getLogger(AutoFollowCoordinator.class);
        final MockLogAppender appender = new MockLogAppender();
        appender.start();
        appender.addExpectation(
                new MockLogAppender.ExceptionSeenEventExpectation(
                        getTestName(),
                        logger.getName(),
                        Level.WARN,
                        "skipping auto-follower coordination",
                        ElasticsearchSecurityException.class,
                        "current license is non-compliant for [ccr]"));
        Loggers.addAppender(logger, appender);
        try {
            assertBusy(appender::assertAllExpectationsMatched);
        } finally {
            Loggers.removeAppender(logger, appender);
            appender.stop();
        }
    }

    private void assertNonCompliantLicense(final Exception e) {
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
