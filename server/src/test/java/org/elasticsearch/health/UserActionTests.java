/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.util.List;

public class UserActionTests extends ESTestCase {
    public void testWriteableSerialization() {
        UserAction.Definition definition = randomDefinition();
        List<String> affectedResources = randomAffectedResources();
        UserAction userAction = new UserAction(definition, randomBoolean() ? null : affectedResources);
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(
            userAction,
            action -> copyWriteable(action, writableRegistry(), UserAction::new),
            this::mutateResult
        );
    }

    private UserAction.Definition randomDefinition() {
        return new UserAction.Definition(
            randomAlphaOfLength(20),
            randomAlphaOfLength(20),
            randomBoolean() ? null : randomAlphaOfLength(20)
        );
    }

    private List<String> randomAffectedResources() {
        return randomList(20, () -> randomAlphaOfLength(20));
    }

    private UserAction mutateResult(UserAction originalUserAction) {
        switch (randomIntBetween(1, 2)) {
            case 1 -> {
                UserAction.Definition newDefinition = randomDefinition();
                return new UserAction(newDefinition, originalUserAction.affectedResources());
            }
            case 2 -> {
                List<String> newAffectedResources = randomAffectedResources();
                return new UserAction(originalUserAction.definition(), newAffectedResources);
            }
            default -> throw new IllegalStateException();
        }
    }
}
