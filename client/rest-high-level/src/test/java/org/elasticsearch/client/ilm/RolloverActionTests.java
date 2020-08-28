/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.ilm;

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
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
        ByteSizeValue maxSize = randomBoolean() ? null : new ByteSizeValue(randomNonNegativeLong() / maxSizeUnit.toBytes(1), maxSizeUnit);
        Long maxDocs = randomBoolean() ? null : randomNonNegativeLong();
        TimeValue maxAge = (maxDocs == null && maxSize == null || randomBoolean())
            ? TimeValue.parseTimeValue(randomPositiveTimeValue(), "rollover_action_test")
            : null;
        return new RolloverAction(maxSize, maxAge, maxDocs);
    }

    public void testNoConditions() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> new RolloverAction(null, null, null));
        assertEquals("At least one rollover condition must be set.", exception.getMessage());
    }
}
