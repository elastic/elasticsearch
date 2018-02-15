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

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class RolloverResponseTests extends ESTestCase {

    public void testFromAndToXContent() throws IOException {
        fromAndToXContentTest(false);
    }

    public void testFromAndToXContentWithRandomFields() throws IOException {
        fromAndToXContentTest(true);
    }

    private void fromAndToXContentTest(boolean addRandomFields) throws IOException {
        RolloverResponse rolloverResponse = createTestItem();
        boolean humanReadable = randomBoolean();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(rolloverResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            mutated = XContentTestUtils.insertRandomFields(xContentType, originalBytes, field -> field.startsWith("conditions"), random());
        } else {
            mutated = originalBytes;
        }

        RolloverResponse parsedRolloverResponse = RolloverResponse.fromXContent(createParser(xContentType.xContent(), mutated));

        assertEquals(rolloverResponse.getNewIndex(), parsedRolloverResponse.getNewIndex());
        assertEquals(rolloverResponse.getOldIndex(), parsedRolloverResponse.getOldIndex());
        assertEquals(rolloverResponse.isRolledOver(), parsedRolloverResponse.isRolledOver());
        assertEquals(rolloverResponse.isDryRun(), parsedRolloverResponse.isDryRun());
        assertEquals(rolloverResponse.isAcknowledged(), parsedRolloverResponse.isAcknowledged());
        assertEquals(rolloverResponse.isShardsAcknowledged(), parsedRolloverResponse.isShardsAcknowledged());
        assertEquals(rolloverResponse.getConditionStatus(), parsedRolloverResponse.getConditionStatus());

        BytesReference finalBytes = toShuffledXContent(parsedRolloverResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        ElasticsearchAssertions.assertToXContentEquivalent(originalBytes, finalBytes, xContentType);
    }

    private static Condition<?> randomCondition() {
        int i = randomIntBetween(0, 2);
        switch(i) {
            case 0:
                return new MaxAgeCondition(new TimeValue(randomNonNegativeLong()));
            case 1:
                return new MaxSizeCondition(new ByteSizeValue(randomNonNegativeLong()));
            case 2:
                return new MaxDocsCondition(randomNonNegativeLong());
            default:
                throw new UnsupportedOperationException();
        }

    }

    private static RolloverResponse createTestItem() {
        Set<Condition.Result> results = new HashSet<>();
        int numResults = randomIntBetween(0, 3);
        for (int i = 0; i < numResults; i++) {
            while (results.add(new Condition.Result(randomCondition(), randomBoolean())) == false) {
            }
        }
        boolean acknowledged = randomBoolean();
        boolean shardsAcknowledged = acknowledged && randomBoolean();
        return new RolloverResponse(randomAlphaOfLengthBetween(3, 10),
                randomAlphaOfLengthBetween(3, 10), results, randomBoolean(), randomBoolean(), acknowledged, shardsAcknowledged);
    }
}
