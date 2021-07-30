/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.indices.rollover;

import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxSizeCondition;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.test.AbstractXContentTestCase.xContentTester;

public class RolloverResponseTests extends ESTestCase {

    private static final List<Supplier<Condition<?>>> conditionSuppliers = new ArrayList<>();
    static {
        conditionSuppliers.add(() -> new MaxAgeCondition(new TimeValue(randomNonNegativeLong())));
        conditionSuppliers.add(() -> new MaxDocsCondition(randomNonNegativeLong()));
        conditionSuppliers.add(() -> new MaxSizeCondition(new ByteSizeValue(randomNonNegativeLong())));
        conditionSuppliers.add(() -> new MaxPrimaryShardSizeCondition(new ByteSizeValue(randomNonNegativeLong())));
    }

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
        List<Supplier<Condition<?>>> conditions = randomSubsetOf(numResults, conditionSuppliers);
        conditions.forEach(condition -> results.put(condition.get().name(), randomBoolean()));

        return new RolloverResponse(oldIndex, newIndex, results, dryRun, rolledOver, acknowledged, shardsAcknowledged);
    }

    private Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.startsWith("conditions");
    }

    private static void toXContent(RolloverResponse response, XContentBuilder builder) throws IOException {
        org.elasticsearch.action.admin.indices.rollover.RolloverResponse serverResponse =
            new org.elasticsearch.action.admin.indices.rollover.RolloverResponse(
                response.getOldIndex(),
                response.getNewIndex(),
                response.getConditionStatus(),
                response.isDryRun(),
                response.isRolledOver(),
                response.isAcknowledged(),
                response.isShardsAcknowledged()
            );
        serverResponse.toXContent(builder, null);
    }
}
