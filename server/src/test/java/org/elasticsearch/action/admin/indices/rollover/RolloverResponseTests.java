/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.rollover;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class RolloverResponseTests extends AbstractWireSerializingTestCase<RolloverResponse> {

    @Override
    protected RolloverResponse createTestInstance() {
        boolean acknowledged = randomBoolean();
        boolean shardsAcknowledged = acknowledged && randomBoolean();
        return new RolloverResponse(
            randomAlphaOfLengthBetween(3, 10),
            randomAlphaOfLengthBetween(3, 10),
            randomResults(true),
            randomBoolean(),
            randomBoolean(),
            acknowledged,
            shardsAcknowledged,
            randomBoolean()
        );
    }

    private static Map<String, Boolean> randomResults(boolean allowNoItems) {
        Map<String, Boolean> results = new HashMap<>();
        int numResults = randomIntBetween(allowNoItems ? 0 : 1, 3);
        List<Supplier<Condition<?>>> conditions = randomSubsetOf(numResults, conditionSuppliers);
        for (Supplier<Condition<?>> condition : conditions) {
            Condition<?> cond = condition.get();
            results.put(cond.name, randomBoolean());
        }
        return results;
    }

    private static final List<Supplier<Condition<?>>> conditionSuppliers = new ArrayList<>();
    static {
        conditionSuppliers.add(() -> new MaxAgeCondition(new TimeValue(randomNonNegativeLong())));
        conditionSuppliers.add(() -> new MaxDocsCondition(randomNonNegativeLong()));
        conditionSuppliers.add(() -> new MaxSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong())));
        conditionSuppliers.add(() -> new MaxPrimaryShardSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong())));
        conditionSuppliers.add(() -> new MaxPrimaryShardDocsCondition(randomNonNegativeLong()));
        conditionSuppliers.add(() -> new MinAgeCondition(new TimeValue(randomNonNegativeLong())));
        conditionSuppliers.add(() -> new MinDocsCondition(randomNonNegativeLong()));
        conditionSuppliers.add(() -> new MinSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong())));
        conditionSuppliers.add(() -> new MinPrimaryShardSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong())));
        conditionSuppliers.add(() -> new MinPrimaryShardDocsCondition(randomNonNegativeLong()));
    }

    @Override
    protected Writeable.Reader<RolloverResponse> instanceReader() {
        return RolloverResponse::new;
    }

    @Override
    protected RolloverResponse mutateInstance(RolloverResponse response) {
        var oldIndex = response.getOldIndex();
        var newIndex = response.getNewIndex();
        var conditionStatus = response.getConditionStatus();
        var dryRun = response.isDryRun();
        var rolledOver = response.isRolledOver();
        var acknowledged = response.isAcknowledged();
        var shardsAcknowledged = response.isShardsAcknowledged();
        var lazy = response.isLazy();
        int i = randomIntBetween(0, 7);
        switch (i) {
            case 0 -> oldIndex = oldIndex + randomAlphaOfLengthBetween(2, 5);
            case 1 -> newIndex = newIndex + randomAlphaOfLengthBetween(2, 5);
            case 2 -> {
                if (response.getConditionStatus().isEmpty()) {
                    conditionStatus = randomResults(false);
                } else {
                    conditionStatus = Maps.newMapWithExpectedSize(response.getConditionStatus().size());
                    List<String> keys = randomSubsetOf(
                        randomIntBetween(1, response.getConditionStatus().size()),
                        response.getConditionStatus().keySet()
                    );
                    for (Map.Entry<String, Boolean> entry : response.getConditionStatus().entrySet()) {
                        boolean value = keys.contains(entry.getKey()) ? entry.getValue() == false : entry.getValue();
                        conditionStatus.put(entry.getKey(), value);
                    }
                }
            }
            case 3 -> dryRun = dryRun == false;
            case 4 -> rolledOver = rolledOver == false;
            case 5 -> {
                acknowledged = response.isAcknowledged() == false;
                shardsAcknowledged = acknowledged && response.isShardsAcknowledged();
            }
            case 6 -> {
                shardsAcknowledged = response.isShardsAcknowledged() == false;
                acknowledged = shardsAcknowledged || response.isAcknowledged();
            }
            case 7 -> lazy = lazy == false;
            default -> throw new UnsupportedOperationException();
        }
        return new RolloverResponse(oldIndex, newIndex, conditionStatus, dryRun, rolledOver, acknowledged, shardsAcknowledged, lazy);
    }
}
