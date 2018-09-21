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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xpack.ccr.action.AutoFollowCoordinator;
import org.elasticsearch.xpack.core.ccr.action.CcrStatsAction;
import org.elasticsearch.xpack.core.ccr.action.CreateAndFollowIndexAction;
import org.elasticsearch.xpack.core.ccr.action.FollowIndexAction;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;

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
        client().execute(CcrStatsAction.INSTANCE, new CcrStatsAction.StatsRequest(), new ActionListener<CcrStatsAction.StatsResponses>() {
            @Override
            public void onResponse(final CcrStatsAction.StatsResponses statsResponses) {
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
        // Update the cluster state so that we have auto follow patterns and verify that we log a warning in case of incompatible license:
        CountDownLatch latch = new CountDownLatch(1);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        clusterService.submitStateUpdateTask("test-add-auto-follow-pattern", new ClusterStateUpdateTask() {

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                AutoFollowPattern autoFollowPattern =
                    new AutoFollowPattern(Collections.singletonList("logs-*"), null, null, null, null, null, null, null, null, null);
                AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(
                    Collections.singletonMap("test_alias", autoFollowPattern),
                    Collections.emptyMap()
                );

                ClusterState.Builder newState = ClusterState.builder(currentState);
                newState.metaData(MetaData.builder(currentState.getMetaData())
                    .putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata)
                    .build());
                return newState.build();
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public void onFailure(String source, Exception e) {
                latch.countDown();
                fail("unexpected error [" + e.getMessage() + "]");
            }
        });
        latch.await();

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
        FollowIndexAction.Request request = new FollowIndexAction.Request();
        request.setLeaderIndex("leader");
        request.setFollowerIndex("follower");
        request.setMaxRetryDelay(TimeValue.timeValueMillis(10));
        request.setPollTimeout(TimeValue.timeValueMillis(10));
        return request;
    }

}
