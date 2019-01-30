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

package org.elasticsearch.client.indices.rollover;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class RolloverResponseTests extends ESTestCase {
    private static List<String> conditionSuppliers = Arrays.asList(
        RolloverRequest.AGE_CONDITION, RolloverRequest.DOCS_CONDITION, RolloverRequest.SIZE_CONDITION);

    public void testFromXContent() throws IOException {
        xContentTester(
            this::createParser,
            RolloverResponseTests::createTestInstance,
            RolloverResponseTests::toXContent,
            RolloverResponse::fromXContent)
            .supportsUnknownFields(true)
            .randomFieldsExcludeFilter(getRandomFieldsExcludeFilter())
            .test();
    }

    private static RolloverResponse createTestInstance() {
        final String oldIndex = randomAlphaOfLength(8);
        final String newIndex = randomAlphaOfLength(8);
        final boolean dryRun = randomBoolean();
        final boolean rolledOver = randomBoolean();
        final boolean acknowledged = randomBoolean();
        final boolean shardsAcknowledged = acknowledged && randomBoolean();

        Map<String, Boolean> results = new HashMap<>();
        int numResults = randomIntBetween(0, 3);
        List<String> conditions = randomSubsetOf(numResults, conditionSuppliers);
        conditions.forEach(condition -> results.put(condition, randomBoolean()));

        return new RolloverResponse(oldIndex, newIndex, results, dryRun, rolledOver, acknowledged, shardsAcknowledged);
    }

    private Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.startsWith("conditions");
    }

    private static void toXContent(RolloverResponse response, XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.field("acknowledged", response.isAcknowledged());
        response.addCustomFields(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
    }
}
