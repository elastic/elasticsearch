/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata;
import org.elasticsearch.xpack.core.ccr.action.ActivateAutoFollowPatternAction.Request;

import java.util.Arrays;
import java.util.Collections;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class TransportActivateAutoFollowPatternActionTests extends ESTestCase {

    public void testInnerActivateNoAutoFollowMetadata() {
        Exception e = expectThrows(
            ResourceNotFoundException.class,
            () -> TransportActivateAutoFollowPatternAction.innerActivate(
                new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test", true),
                ClusterState.EMPTY_STATE
            )
        );
        assertThat(e.getMessage(), equalTo("auto-follow pattern [test] is missing"));
    }

    public void testInnerActivateDoesNotExist() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        AutoFollowMetadata.TYPE,
                        new AutoFollowMetadata(
                            singletonMap("remote_cluster", randomAutoFollowPattern()),
                            singletonMap("remote_cluster", randomSubsetOf(randomIntBetween(1, 3), "uuid0", "uuid1", "uuid2")),
                            singletonMap("remote_cluster", singletonMap("header0", randomFrom("val0", "val2", "val3")))
                        )
                    )
            )
            .build();
        Exception e = expectThrows(
            ResourceNotFoundException.class,
            () -> TransportActivateAutoFollowPatternAction.innerActivate(
                new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "does_not_exist", true),
                clusterState
            )
        );
        assertThat(e.getMessage(), equalTo("auto-follow pattern [does_not_exist] is missing"));
    }

    public void testInnerActivateToggle() {
        final AutoFollowMetadata.AutoFollowPattern autoFollowPattern = randomAutoFollowPattern();
        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        AutoFollowMetadata.TYPE,
                        new AutoFollowMetadata(
                            singletonMap("remote_cluster", autoFollowPattern),
                            singletonMap("remote_cluster", randomSubsetOf(randomIntBetween(1, 3), "uuid0", "uuid1", "uuid2")),
                            singletonMap("remote_cluster", singletonMap("header0", randomFrom("val0", "val2", "val3")))
                        )
                    )
            )
            .build();
        {
            Request pauseRequest = new Request(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "remote_cluster", autoFollowPattern.isActive());
            ClusterState updatedState = TransportActivateAutoFollowPatternAction.innerActivate(pauseRequest, clusterState);
            assertThat(updatedState, sameInstance(clusterState));
        }
        {
            Request pauseRequest = new Request(
                TEST_REQUEST_TIMEOUT,
                TEST_REQUEST_TIMEOUT,
                "remote_cluster",
                autoFollowPattern.isActive() == false
            );
            ClusterState updatedState = TransportActivateAutoFollowPatternAction.innerActivate(pauseRequest, clusterState);
            assertThat(updatedState, not(sameInstance(clusterState)));

            AutoFollowMetadata updatedAutoFollowMetadata = updatedState.getMetadata().getProject().custom(AutoFollowMetadata.TYPE);
            assertNotEquals(updatedAutoFollowMetadata, notNullValue());

            AutoFollowMetadata autoFollowMetadata = clusterState.getMetadata().getProject().custom(AutoFollowMetadata.TYPE);
            assertNotEquals(updatedAutoFollowMetadata, autoFollowMetadata);
            assertThat(updatedAutoFollowMetadata.getPatterns().size(), equalTo(autoFollowMetadata.getPatterns().size()));
            assertThat(updatedAutoFollowMetadata.getPatterns().get("remote_cluster").isActive(), not(autoFollowPattern.isActive()));

            assertEquals(updatedAutoFollowMetadata.getFollowedLeaderIndexUUIDs(), autoFollowMetadata.getFollowedLeaderIndexUUIDs());
            assertEquals(updatedAutoFollowMetadata.getHeaders(), autoFollowMetadata.getHeaders());
        }
    }

    private static AutoFollowMetadata.AutoFollowPattern randomAutoFollowPattern() {
        return new AutoFollowMetadata.AutoFollowPattern(
            randomAlphaOfLength(5),
            randomSubsetOf(Arrays.asList("test-*", "user-*", "logs-*", "failures-*")),
            Collections.emptyList(),
            randomFrom("{{leader_index}}", "{{leader_index}}-follower", "test"),
            Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 4)).build(),
            randomBoolean(),
            randomIntBetween(1, 100),
            randomIntBetween(1, 100),
            randomIntBetween(1, 100),
            randomIntBetween(1, 100),
            ByteSizeValue.of(randomIntBetween(1, 100), randomFrom(ByteSizeUnit.values())),
            ByteSizeValue.of(randomIntBetween(1, 100), randomFrom(ByteSizeUnit.values())),
            randomIntBetween(1, 100),
            ByteSizeValue.of(randomIntBetween(1, 100), randomFrom(ByteSizeUnit.values())),
            TimeValue.timeValueSeconds(randomIntBetween(30, 600)),
            TimeValue.timeValueSeconds(randomIntBetween(30, 600))
        );
    }
}
