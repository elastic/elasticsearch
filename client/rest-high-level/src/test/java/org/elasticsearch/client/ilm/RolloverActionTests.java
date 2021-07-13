/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

public class RolloverActionTests extends AbstractXContentTestCase<RolloverAction> {

    @Override
    protected RolloverAction doParseInstance(XContentParser parser) {
        return RolloverAction.parse(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected RolloverAction createTestInstance() {
        return randomInstance();
    }

    static RolloverAction randomInstance() {
        ByteSizeUnit maxSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue maxSize = randomBoolean()
            ? null : new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
        ByteSizeUnit maxPrimaryShardSizeUnit = randomFrom(ByteSizeUnit.values());
        ByteSizeValue maxPrimaryShardSize = randomBoolean()
            ? null : new ByteSizeValue(randomNonNegativeLong() / maxPrimaryShardSizeUnit.toBytes(1), maxPrimaryShardSizeUnit);
        TimeValue maxAge = randomBoolean()
            ? null : TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test");
        Long maxDocs = (maxSize == null && maxPrimaryShardSize == null && maxAge == null || randomBoolean())
            ? randomNonNegativeLong() : null;
        return new RolloverAction(maxSize, maxPrimaryShardSize, maxAge, maxDocs);
    }

    public void testNoConditions() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> new RolloverAction(null, null, null, null));
        assertEquals("At least one rollover condition must be set.", exception.getMessage());
    }
}
