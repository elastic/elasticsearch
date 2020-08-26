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

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
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
        return new RolloverResponse(randomAlphaOfLengthBetween(3, 10),
                randomAlphaOfLengthBetween(3, 10), randomResults(true), randomBoolean(), randomBoolean(), acknowledged, shardsAcknowledged);
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
        conditionSuppliers.add(() -> new MaxSizeCondition(new ByteSizeValue(randomNonNegativeLong())));
        conditionSuppliers.add(() -> new MaxDocsCondition(randomNonNegativeLong()));
    }

    @Override
    protected Writeable.Reader<RolloverResponse> instanceReader() {
        return RolloverResponse::new;
    }

    @Override
    protected RolloverResponse mutateInstance(RolloverResponse response) {
        int i = randomIntBetween(0, 6);
        switch(i) {
            case 0:
                return new RolloverResponse(response.getOldIndex() + randomAlphaOfLengthBetween(2, 5),
                        response.getNewIndex(), response.getConditionStatus(), response.isDryRun(), response.isRolledOver(),
                        response.isAcknowledged(), response.isShardsAcknowledged());
            case 1:
                return new RolloverResponse(response.getOldIndex(), response.getNewIndex() + randomAlphaOfLengthBetween(2, 5),
                        response.getConditionStatus(), response.isDryRun(), response.isRolledOver(),
                        response.isAcknowledged(), response.isShardsAcknowledged());
            case 2:
                Map<String, Boolean> results;
                if (response.getConditionStatus().isEmpty()) {
                    results = randomResults(false);
                } else {
                    results = new HashMap<>(response.getConditionStatus().size());
                    List<String> keys = randomSubsetOf(randomIntBetween(1, response.getConditionStatus().size()),
                            response.getConditionStatus().keySet());
                    for (Map.Entry<String, Boolean> entry : response.getConditionStatus().entrySet()) {
                        boolean value = keys.contains(entry.getKey()) ? entry.getValue() == false : entry.getValue();
                        results.put(entry.getKey(), value);
                    }
                }
                return new RolloverResponse(response.getOldIndex(), response.getNewIndex(), results, response.isDryRun(),
                        response.isRolledOver(), response.isAcknowledged(), response.isShardsAcknowledged());
            case 3:
                return new RolloverResponse(response.getOldIndex(), response.getNewIndex(),
                        response.getConditionStatus(), response.isDryRun() == false, response.isRolledOver(),
                        response.isAcknowledged(), response.isShardsAcknowledged());
            case 4:
                return new RolloverResponse(response.getOldIndex(), response.getNewIndex(),
                        response.getConditionStatus(), response.isDryRun(), response.isRolledOver() == false,
                        response.isAcknowledged(), response.isShardsAcknowledged());
            case 5: {
                boolean acknowledged = response.isAcknowledged() == false;
                boolean shardsAcknowledged = acknowledged && response.isShardsAcknowledged();
                return new RolloverResponse(response.getOldIndex(), response.getNewIndex(),
                        response.getConditionStatus(), response.isDryRun(), response.isRolledOver(),
                        acknowledged, shardsAcknowledged);
            }
            case 6: {
                boolean shardsAcknowledged = response.isShardsAcknowledged() == false;
                boolean acknowledged = shardsAcknowledged || response.isAcknowledged();
                return new RolloverResponse(response.getOldIndex(), response.getNewIndex(),
                        response.getConditionStatus(), response.isDryRun(), response.isRolledOver(),
                        acknowledged, shardsAcknowledged);
            }
            default:
                throw new UnsupportedOperationException();
        }
    }
}
