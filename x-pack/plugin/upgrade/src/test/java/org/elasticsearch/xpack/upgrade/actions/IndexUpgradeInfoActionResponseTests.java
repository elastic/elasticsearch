/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.upgrade.actions;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.upgrade.UpgradeActionRequired;
import org.elasticsearch.xpack.core.upgrade.actions.IndexUpgradeInfoAction.Response;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class IndexUpgradeInfoActionResponseTests extends AbstractStreamableTestCase<Response> {


    @Override
    protected Response createTestInstance() {
        int actionsCount = randomIntBetween(0, 5);
        Map<String, UpgradeActionRequired> actions = new HashMap<>(actionsCount);
        for (int i = 0; i < actionsCount; i++) {
            actions.put(randomAlphaOfLength(10), randomFrom(EnumSet.allOf(UpgradeActionRequired.class)));
        }
        return new Response(actions);
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }
}
