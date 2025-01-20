/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.TransportClearScrollAction;
import org.elasticsearch.action.support.single.shard.SingleShardRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;

public class SecurityActionMapperTests extends ESTestCase {

    public void testThatAllOrdinaryActionsRemainTheSame() {
        StringBuilder actionNameBuilder = new StringBuilder();
        if (randomBoolean()) {
            actionNameBuilder.append("indices:");
            if (randomBoolean()) {
                actionNameBuilder.append("data/");
                actionNameBuilder.append(randomBoolean() ? "read" : "write");
                actionNameBuilder.append("/");
                actionNameBuilder.append(randomAlphaOfLengthBetween(2, 12));
            } else {
                actionNameBuilder.append(randomBoolean() ? "admin" : "monitor");
                actionNameBuilder.append("/");
                actionNameBuilder.append(randomAlphaOfLengthBetween(2, 12));
            }
        } else {
            actionNameBuilder.append("cluster:");
            actionNameBuilder.append(randomBoolean() ? "admin" : "monitor");
            actionNameBuilder.append("/");
            actionNameBuilder.append(randomAlphaOfLengthBetween(2, 12));
        }
        String randomAction = actionNameBuilder.toString();
        assumeFalse(
            "Random action is one of the known mapped values: " + randomAction,
            randomAction.equals(TransportClearScrollAction.NAME)
                || randomAction.equals(AnalyzeAction.NAME)
                || randomAction.equals(AnalyzeAction.NAME + "[s]")
        );

        assertThat(SecurityActionMapper.action(randomAction, null), equalTo(randomAction));
    }

    public void testClearScroll() {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        int scrollIds = randomIntBetween(1, 10);
        for (int i = 0; i < scrollIds; i++) {
            clearScrollRequest.addScrollId(randomAlphaOfLength(randomIntBetween(1, 30)));
        }
        assertThat(
            SecurityActionMapper.action(TransportClearScrollAction.NAME, clearScrollRequest),
            equalTo(TransportClearScrollAction.NAME)
        );
    }

    public void testClearScrollAll() {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        int scrollIds = randomIntBetween(0, 10);
        for (int i = 0; i < scrollIds; i++) {
            clearScrollRequest.addScrollId(randomAlphaOfLength(randomIntBetween(1, 30)));
        }
        clearScrollRequest.addScrollId("_all");
        // make sure that wherever the _all is among the scroll ids the action name gets translated
        Collections.shuffle(clearScrollRequest.getScrollIds(), random());

        assertThat(
            SecurityActionMapper.action(TransportClearScrollAction.NAME, clearScrollRequest),
            equalTo(SecurityActionMapper.CLUSTER_PERMISSION_SCROLL_CLEAR_ALL_NAME)
        );
    }

    public void testIndicesAnalyze() {
        AnalyzeAction.Request analyzeRequest;
        if (randomBoolean()) {
            analyzeRequest = new AnalyzeAction.Request(randomAlphaOfLength(randomIntBetween(1, 30))).text("text");
        } else {
            analyzeRequest = new AnalyzeAction.Request(null).text("text");
            analyzeRequest.index(randomAlphaOfLength(randomIntBetween(1, 30)));
        }
        assertThat(SecurityActionMapper.action(AnalyzeAction.NAME, analyzeRequest), equalTo(AnalyzeAction.NAME));
    }

    public void testClusterAnalyze() {
        AnalyzeAction.Request analyzeRequest = new AnalyzeAction.Request(null).text("text");
        assertThat(
            SecurityActionMapper.action(AnalyzeAction.NAME, analyzeRequest),
            equalTo(SecurityActionMapper.CLUSTER_PERMISSION_ANALYZE)
        );
    }

    public void testPainlessExecuteWithIndex() {
        MockPainlessExecuteRequest withIndex = new MockPainlessExecuteRequest("index");
        assertThat(
            SecurityActionMapper.action(SecurityActionMapper.CLUSTER_PERMISSION_PAINLESS_EXECUTE, withIndex),
            equalTo("indices:data/read/scripts/painless/execute")
        );
        assertThat(
            SecurityActionMapper.action(SecurityActionMapper.CLUSTER_PERMISSION_PAINLESS_EXECUTE + "[s]", withIndex),
            equalTo("indices:data/read/scripts/painless/execute[s]")
        );
    }

    public void testPainlessExecuteWithoutIndex() {
        MockPainlessExecuteRequest withoutIndex = new MockPainlessExecuteRequest(null);
        assertThat(
            SecurityActionMapper.action(SecurityActionMapper.CLUSTER_PERMISSION_PAINLESS_EXECUTE, withoutIndex),
            equalTo(SecurityActionMapper.CLUSTER_PERMISSION_PAINLESS_EXECUTE)
        );
    }

    private static class MockPainlessExecuteRequest extends SingleShardRequest<MockPainlessExecuteRequest> {
        MockPainlessExecuteRequest(String index) {
            super(index);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }
}
