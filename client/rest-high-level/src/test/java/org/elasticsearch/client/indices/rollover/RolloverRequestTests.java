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
        MaxDocsCondition maxDocsCondition = new MaxDocsCondition(10000L);
        MaxSizeCondition maxSizeCondition = new MaxSizeCondition(new ByteSizeValue(2000));
        MaxPrimaryShardSizeCondition maxPrimaryShardSizeCondition = new MaxPrimaryShardSizeCondition(new ByteSizeValue(3000));
        Condition<?>[] expectedConditions = new Condition<?>[]{
            maxAgeCondition, maxDocsCondition, maxSizeCondition, maxPrimaryShardSizeCondition
        };
        rolloverRequest.addMaxIndexAgeCondition(maxAgeCondition.value());
        rolloverRequest.addMaxIndexDocsCondition(maxDocsCondition.value());
        rolloverRequest.addMaxIndexSizeCondition(maxSizeCondition.value());
        rolloverRequest.addMaxPrimaryShardSizeCondition(maxPrimaryShardSizeCondition.value());
        List<Condition<?>> requestConditions = new ArrayList<>(rolloverRequest.getConditions().values());
        assertThat(requestConditions, containsInAnyOrder(expectedConditions));
    }

    public void testValidation() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () ->
            new RolloverRequest(null, null));
        assertEquals("The index alias cannot be null!", exception.getMessage());
    }
}
