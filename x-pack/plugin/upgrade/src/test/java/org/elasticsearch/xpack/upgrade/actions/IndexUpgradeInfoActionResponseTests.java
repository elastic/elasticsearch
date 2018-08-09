/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade.actions;

import org.elasticsearch.protocol.xpack.migration.IndexUpgradeInfoResponse;
import org.elasticsearch.protocol.xpack.migration.UpgradeActionRequired;
import org.elasticsearch.test.AbstractStreamableTestCase;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class IndexUpgradeInfoActionResponseTests extends AbstractStreamableTestCase<IndexUpgradeInfoResponse> {


    @Override
    protected IndexUpgradeInfoResponse createTestInstance() {
        int actionsCount = randomIntBetween(0, 5);
        Map<String, UpgradeActionRequired> actions = new HashMap<>(actionsCount);
        for (int i = 0; i < actionsCount; i++) {
            actions.put(randomAlphaOfLength(10), randomFrom(EnumSet.allOf(UpgradeActionRequired.class)));
        }
        return new IndexUpgradeInfoResponse(actions);
    }

    @Override
    protected IndexUpgradeInfoResponse createBlankInstance() {
        return new IndexUpgradeInfoResponse();
    }
}
