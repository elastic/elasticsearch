/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeAction;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;

import static org.hamcrest.CoreMatchers.equalTo;

public class SecurityActionMapperTests extends ESTestCase {

    public void testThatAllOrdinaryActionsRemainTheSame() {
        SecurityActionMapper securityActionMapper = new SecurityActionMapper();
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
        assumeFalse("Random action is one of the known mapped values: " + randomAction, randomAction.equals(ClearScrollAction.NAME) ||
                    randomAction.equals(AnalyzeAction.NAME) ||
                    randomAction.equals(AnalyzeAction.NAME + "[s]"));

        assertThat(securityActionMapper.action(randomAction, null), equalTo(randomAction));
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
        AnalyzeAction.Request analyzeRequest;
        if (randomBoolean()) {
            analyzeRequest = new AnalyzeAction.Request(randomAlphaOfLength(randomIntBetween(1, 30))).text("text");
        } else {
            analyzeRequest = new AnalyzeAction.Request(null).text("text");
            analyzeRequest.index(randomAlphaOfLength(randomIntBetween(1, 30)));
        }
        assertThat(securityActionMapper.action(AnalyzeAction.NAME, analyzeRequest), equalTo(AnalyzeAction.NAME));
    }

    public void testClusterAnalyze() {
        SecurityActionMapper securityActionMapper = new SecurityActionMapper();
        AnalyzeAction.Request analyzeRequest = new AnalyzeAction.Request(null).text("text");
        assertThat(securityActionMapper.action(AnalyzeAction.NAME, analyzeRequest),
                equalTo(SecurityActionMapper.CLUSTER_PERMISSION_ANALYZE));
    }
}
