/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class GetAutoFollowPatternResponseTests extends AbstractStreamableTestCase<GetAutoFollowPatternAction.Response> {

    @Override
    protected GetAutoFollowPatternAction.Response createBlankInstance() {
        return new GetAutoFollowPatternAction.Response();
    }

    @Override
    protected GetAutoFollowPatternAction.Response createTestInstance() {
        int numPatterns = randomIntBetween(1, 8);
        Map<String, AutoFollowPattern> patterns = new HashMap<>(numPatterns);
        for (int i = 0; i < numPatterns; i++) {
            AutoFollowPattern autoFollowPattern = new AutoFollowPattern(
                Collections.singletonList(randomAlphaOfLength(4)),
                randomAlphaOfLength(4),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomNonNegativeLong(),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                TimeValue.timeValueMillis(500),
                TimeValue.timeValueMillis(500));
            patterns.put(randomAlphaOfLength(4), autoFollowPattern);
        }
        return new GetAutoFollowPatternAction.Response(patterns);
    }
}
