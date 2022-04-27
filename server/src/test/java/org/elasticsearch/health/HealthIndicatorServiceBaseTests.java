/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HealthIndicatorServiceBaseTests extends ESTestCase {

    private static final String TEST_INDICATOR_NAME = "test";
    private static final String TEST_INDICATOR_COMPONENT = "testing";

    /**
     * Dummy implementation that returns a predefined result when state is known.
     */
    private static class TestBaseIndicatorService extends HealthIndicatorServiceBase {
        private final HealthIndicatorResult result;

        TestBaseIndicatorService(ClusterService clusterService, HealthIndicatorResult result) {
            super(clusterService);
            this.result = result;
        }

        @Override
        public String name() {
            return TEST_INDICATOR_NAME;
        }

        @Override
        public String component() {
            return TEST_INDICATOR_COMPONENT;
        }

        @Override
        protected HealthIndicatorResult doCalculate(ClusterState clusterState, boolean calculateDetails) {
            return result;
        }
    }

    private static final HealthIndicatorResult SIMPLE_RESULT = new HealthIndicatorResult(
        TEST_INDICATOR_NAME,
        TEST_INDICATOR_COMPONENT,
        HealthStatus.GREEN,
        "",
        HealthIndicatorDetails.EMPTY,
        Collections.emptyList()
    );

    public void testDoCalculate() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster")).build();
        TestBaseIndicatorService indicator = createIndicator(clusterState, SIMPLE_RESULT);
        HealthIndicatorResult actualResult = indicator.calculate(randomBoolean());
        assertThat(actualResult, is(sameInstance(SIMPLE_RESULT)));
    }

    public void testDoCalculateUnknownNotRecovered() throws IOException {
        runCalculateUnknownTest(GatewayService.STATE_NOT_RECOVERED_BLOCK, true, false);
    }

    public void testDoCalculateUnknownNoMaster() throws IOException {
        runCalculateUnknownTest(
            randomFrom(
                NoMasterBlockService.NO_MASTER_BLOCK_ALL,
                NoMasterBlockService.NO_MASTER_BLOCK_WRITES,
                NoMasterBlockService.NO_MASTER_BLOCK_METADATA_WRITES
            ),
            false,
            true
        );
    }

    public void runCalculateUnknownTest(ClusterBlock block, boolean hasMaster, boolean clusterStateRecovered) throws IOException {
        ClusterState clusterState = ClusterState.builder(new ClusterName("test-cluster"))
            .blocks(ClusterBlocks.builder().addGlobalBlock(block))
            .build();
        boolean detailed = randomBoolean();

        TestBaseIndicatorService indicator = createIndicator(clusterState, SIMPLE_RESULT);
        HealthIndicatorResult actualResult = indicator.calculate(detailed);

        assertThat(actualResult, is(not(sameInstance(SIMPLE_RESULT))));
        assertThat(actualResult.name(), is(equalTo(TEST_INDICATOR_NAME)));
        assertThat(actualResult.component(), is(equalTo(TEST_INDICATOR_COMPONENT)));
        assertThat(actualResult.status(), is(equalTo(HealthStatus.UNKNOWN)));
        assertThat(actualResult.summary(), is(equalTo(HealthIndicatorServiceBase.COULD_NOT_DETERMINE_HEALTH)));

        if (detailed) {
            HealthIndicatorDetails details = actualResult.details();
            assertThat(details, not(sameInstance(HealthIndicatorDetails.EMPTY)));

            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            details.toXContent(builder, ToXContent.EMPTY_PARAMS);
            Map<String, Object> detailMap = XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2();
            assertThat(detailMap, is(not(anEmptyMap())));
            assertThat(detailMap, hasKey("reason"));

            Map<String, Object> expectedReasons = new HashMap<>();
            expectedReasons.put("has_master", hasMaster);
            expectedReasons.put("cluster_state_recovered", clusterStateRecovered);
            assertThat(detailMap.get("reason"), is(equalTo(expectedReasons)));
        } else {
            assertThat(actualResult.details(), sameInstance(HealthIndicatorDetails.EMPTY));
        }
    }

    private static TestBaseIndicatorService createIndicator(ClusterState clusterState, HealthIndicatorResult result) {
        var clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        return new TestBaseIndicatorService(clusterService, result);
    }
}
