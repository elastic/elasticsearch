/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ccr.AutoFollowMetadata.AutoFollowPattern;
import org.elasticsearch.xpack.core.ccr.action.GetAutoFollowPatternAction;

import java.util.Collections;
import java.util.Map;

public class GetAutoFollowPatternResponseTests extends AbstractWireSerializingTestCase<GetAutoFollowPatternAction.Response> {

    @Override
    protected Writeable.Reader<GetAutoFollowPatternAction.Response> instanceReader() {
        return GetAutoFollowPatternAction.Response::new;
    }

    @Override
    protected GetAutoFollowPatternAction.Response createTestInstance() {
        int numPatterns = randomIntBetween(1, 8);
        Map<String, AutoFollowPattern> patterns = Maps.newMapWithExpectedSize(numPatterns);
        for (int i = 0; i < numPatterns; i++) {
            AutoFollowPattern autoFollowPattern = new AutoFollowPattern(
                "remote",
                Collections.singletonList(randomAlphaOfLength(4)),
                Collections.singletonList(randomAlphaOfLength(4)),
                randomAlphaOfLength(4),
                Settings.builder().put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), randomIntBetween(0, 4)).build(),
                true,
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                randomIntBetween(0, Integer.MAX_VALUE),
                new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES),
                new ByteSizeValue(randomNonNegativeLong(), ByteSizeUnit.BYTES),
                randomIntBetween(0, Integer.MAX_VALUE),
                new ByteSizeValue(randomNonNegativeLong()),
                TimeValue.timeValueMillis(500),
                TimeValue.timeValueMillis(500)
            );
            patterns.put(randomAlphaOfLength(4), autoFollowPattern);
        }
        return new GetAutoFollowPatternAction.Response(patterns);
    }
}
