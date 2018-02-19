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

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class RolloverResponseTests extends AbstractStreamableXContentTestCase<RolloverResponse> {

    @Override
    protected RolloverResponse createTestInstance() {
        Set<Condition.Result> results = new HashSet<>();
        int numResults = randomIntBetween(0, 3);
        List<Supplier<Condition<?>>> conditions = randomSubsetOf(numResults, conditionSuppliers);
        for (Supplier<Condition<?>> condition : conditions) {
            results.add(new Condition.Result(condition.get(), randomBoolean()));
        }
        boolean acknowledged = randomBoolean();
        boolean shardsAcknowledged = acknowledged && randomBoolean();
        return new RolloverResponse(randomAlphaOfLengthBetween(3, 10),
                randomAlphaOfLengthBetween(3, 10), results, randomBoolean(), randomBoolean(), acknowledged, shardsAcknowledged);
    }

    private static final List<Supplier<Condition<?>>> conditionSuppliers = new ArrayList<>();
    static {
        conditionSuppliers.add(() -> new MaxAgeCondition(new TimeValue(randomNonNegativeLong())));
        conditionSuppliers.add(() -> new MaxDocsCondition(randomNonNegativeLong()));
        conditionSuppliers.add(() -> new MaxDocsCondition(randomNonNegativeLong()));
    }

    @Override
    protected RolloverResponse createBlankInstance() {
        return new RolloverResponse();
    }

    @Override
    protected RolloverResponse doParseInstance(XContentParser parser) {
        return RolloverResponse.fromXContent(parser);
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> field.startsWith("conditions");
    }
}
