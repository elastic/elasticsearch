/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.xpack.CcrSingleNodeTestCase;
import org.elasticsearch.xpack.ccr.action.AutoFollowCoordinator;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.action.FollowStatsAction;
import org.elasticsearch.xpack.core.ccr.action.PutAutoFollowPatternAction;
import org.elasticsearch.xpack.core.ccr.action.PutFollowAction;
import org.elasticsearch.xpack.core.ccr.action.ResumeFollowAction;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CcrLicenseIT extends CcrSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(NonCompliantLicenseLocalStateCcr.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.EMPTY;
    }

    public void testThatFollowingIndexIsUnavailableWithNonCompliantLicense() throws InterruptedException {
        final ResumeFollowAction.Request followRequest = getResumeFollowRequest("follower");
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(ResumeFollowAction.INSTANCE, followRequest, new ActionListener<AcknowledgedResponse>() {
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
        final PutFollowAction.Request createAndFollowRequest = getPutFollowRequest("leader", "follower");
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(PutFollowAction.INSTANCE, createAndFollowRequest, new ActionListener<PutFollowAction.Response>() {
            @Override
            public void onResponse(final PutFollowAction.Response response) {
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

    public void testThatFollowStatsAreUnavailableWithNonCompliantLicense() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(
            FollowStatsAction.INSTANCE,
            new FollowStatsAction.StatsRequest(),
            new ActionListener<FollowStatsAction.StatsResponses>() {
                @Override
                public void onResponse(final FollowStatsAction.StatsResponses statsResponses) {
                    latch.countDown();
                    fail();
                }

                @Override
                public void onFailure(final Exception e) {
                    assertNonCompliantLicense(e);
                    latch.countDown();
                }
            }
        );

        latch.await();
    }

    public void testThatPutAutoFollowPatternsIsUnavailableWithNonCompliantLicense() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setName("name");
        request.setRemoteCluster("leader");
        request.setLeaderIndexPatterns(Collections.singletonList("*"));
        client().execute(PutAutoFollowPatternAction.INSTANCE, request, new ActionListener<AcknowledgedResponse>() {
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
                "current license is non-compliant for [ccr]"
            )
        );

        try {
            // Need to add mock log appender before submitting CS update, otherwise we miss the expected log:
            // (Auto followers for new remote clusters are bootstrapped when a new cluster state is published)
            Loggers.addAppender(logger, appender);
            // Update the cluster state so that we have auto follow patterns and verify that we log a warning
            // in case of incompatible license:
            CountDownLatch latch = new CountDownLatch(1);
            ClusterService clusterService = getInstanceFromNode(ClusterService.class);
            clusterService.submitUnbatchedStateUpdateTask("test-add-auto-follow-pattern", new ClusterStateUpdateTask() {

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    AutoFollowPattern autoFollowPattern = new AutoFollowPattern(
                        "test_alias",
                        Collections.singletonList("logs-*"),
                        Collections.emptyList(),
                        null,
                        Settings.EMPTY,
                        true,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null
                    );
                    AutoFollowMetadata autoFollowMetadata = new AutoFollowMetadata(
                        Collections.singletonMap("test_alias", autoFollowPattern),
                        Collections.emptyMap(),
                        Collections.emptyMap()
                    );

                    ClusterState.Builder newState = ClusterState.builder(currentState);
                    newState.metadata(
                        Metadata.builder(currentState.getMetadata()).putCustom(AutoFollowMetadata.TYPE, autoFollowMetadata).build()
                    );
                    return newState.build();
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    latch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    latch.countDown();
                    fail("unexpected error [" + e.getMessage() + "]");
                }
            });
            latch.await();
            appender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(logger, appender);
            appender.stop();
        }
    }

    private void assertNonCompliantLicense(final Exception e) {
        assertThat(e, instanceOf(ElasticsearchSecurityException.class));
        assertThat(e.getMessage(), equalTo("current license is non-compliant for [ccr]"));
    }

}
