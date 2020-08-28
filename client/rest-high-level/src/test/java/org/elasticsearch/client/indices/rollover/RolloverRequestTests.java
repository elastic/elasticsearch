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

import org.elasticsearch.action.admin.indices.rollover.Condition;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxSizeCondition;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsInAnyOrder;


public class RolloverRequestTests extends ESTestCase {
    public void testConstructorAndFieldAssignments() {
        // test constructor
        String alias = randomAlphaOfLength(5);
        String newIndexName = null;
        if (randomBoolean()) {
            newIndexName = randomAlphaOfLength(8);
        }
        RolloverRequest rolloverRequest = new RolloverRequest(alias, newIndexName);
        assertEquals(alias, rolloverRequest.getAlias());
        assertEquals(newIndexName, rolloverRequest.getNewIndexName());

        // test assignment of conditions
        MaxAgeCondition maxAgeCondition = new MaxAgeCondition(new TimeValue(10));
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(2000));
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(10000L);
        Condition<?>[] expectedConditions = new Condition<?>[] {maxAgeCondition, maxSizeCondition, maxDocsCondition};
        rolloverRequest.addMaxIndexAgeCondition(maxAgeCondition.value());
        rolloverRequest.addMaxIndexSizeCondition(maxSizeCondition.value());
        rolloverRequest.addMaxIndexDocsCondition(maxDocsCondition.value());
        List<Condition<?>> requestConditions = new ArrayList<>(rolloverRequest.getConditions().values());
        assertThat(requestConditions, containsInAnyOrder(expectedConditions));
    }

    public void testValidation() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            new RolloverRequest(null, null));
        assertEquals("The index alias cannot be null!", exception.getMessage());
    }
}
