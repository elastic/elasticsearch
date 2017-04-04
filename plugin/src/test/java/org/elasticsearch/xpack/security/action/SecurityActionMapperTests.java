/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.KnownActionsTests;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;

public class SecurityActionMapperTests extends ESTestCase {

    public void testThatAllOrdinaryActionsRemainTheSame() {
        List<String> actions = new ArrayList<>();
        actions.addAll(KnownActionsTests.loadKnownActions());
        actions.addAll(KnownActionsTests.loadKnownHandlers());

        SecurityActionMapper securityActionMapper = new SecurityActionMapper();
        int iterations = randomIntBetween(10, 100);
        for (int i = 0; i < iterations; i++) {
            String randomAction;
            do {
                if (randomBoolean()) {
                    randomAction = randomFrom(actions);
                } else {
                    randomAction = randomAlphaOfLength(randomIntBetween(1, 30));
                }
            } while (randomAction.equals(ClearScrollAction.NAME) ||
                    randomAction.equals(AnalyzeAction.NAME) ||
                    randomAction.equals(AnalyzeAction.NAME + "[s]"));

            assertThat(securityActionMapper.action(randomAction, null), equalTo(randomAction));
        }
    }

    public void testClearScroll() {
        SecurityActionMapper securityActionMapper = new SecurityActionMapper();
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        int scrollIds = randomIntBetween(1, 10);
        for (int i = 0; i < scrollIds; i++) {
            clearScrollRequest.addScrollId(randomAlphaOfLength(randomIntBetween(1, 30)));
        }
        assertThat(securityActionMapper.action(ClearScrollAction.NAME, clearScrollRequest), equalTo(ClearScrollAction.NAME));
    }

    public void testClearScrollAll() {
        SecurityActionMapper securityActionMapper = new SecurityActionMapper();
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        int scrollIds = randomIntBetween(0, 10);
        for (int i = 0; i < scrollIds; i++) {
            clearScrollRequest.addScrollId(randomAlphaOfLength(randomIntBetween(1, 30)));
        }
        clearScrollRequest.addScrollId("_all");
        //make sure that wherever the _all is among the scroll ids the action name gets translated
        Collections.shuffle(clearScrollRequest.getScrollIds(), random());

        assertThat(securityActionMapper.action(ClearScrollAction.NAME, clearScrollRequest),
                equalTo(SecurityActionMapper.CLUSTER_PERMISSION_SCROLL_CLEAR_ALL_NAME));
    }

    public void testIndicesAnalyze() {
        SecurityActionMapper securityActionMapper = new SecurityActionMapper();
        AnalyzeRequest analyzeRequest;
        if (randomBoolean()) {
            analyzeRequest = new AnalyzeRequest(randomAlphaOfLength(randomIntBetween(1, 30))).text("text");
        } else {
            analyzeRequest = new AnalyzeRequest(null).text("text");
            analyzeRequest.index(randomAlphaOfLength(randomIntBetween(1, 30)));
        }
        assertThat(securityActionMapper.action(AnalyzeAction.NAME, analyzeRequest), equalTo(AnalyzeAction.NAME));
    }

    public void testClusterAnalyze() {
        SecurityActionMapper securityActionMapper = new SecurityActionMapper();
        AnalyzeRequest analyzeRequest = new AnalyzeRequest(null).text("text");
        assertThat(securityActionMapper.action(AnalyzeAction.NAME, analyzeRequest),
                equalTo(SecurityActionMapper.CLUSTER_PERMISSION_ANALYZE));
    }
}
