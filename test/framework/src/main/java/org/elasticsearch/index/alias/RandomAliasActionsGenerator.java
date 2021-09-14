/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.alias;

import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomLong;

public final class RandomAliasActionsGenerator {

    private RandomAliasActionsGenerator() {}

    public static AliasActions randomAliasAction() {
        return randomAliasAction(false);
    }

    public static AliasActions randomAliasAction(boolean useStringAsFilter) {
        AliasActions action = new AliasActions(randomFrom(AliasActions.Type.values()));
        if (randomBoolean()) {
            action.index(randomAlphaOfLength(5));
        } else {
            int numIndices = randomIntBetween(1, 5);
            String[] indices = new String[numIndices];
            for (int i = 0; i < numIndices; i++) {
                indices[i] = "index-" + randomAlphaOfLengthBetween(2, 5).toLowerCase(Locale.ROOT);
            }
            action.indices(indices);
        }
        if (action.actionType() == AliasActions.Type.REMOVE) {
            if (randomBoolean()) {
                action.mustExist(randomBoolean());
            }
        }
        if (action.actionType() != AliasActions.Type.REMOVE_INDEX) {
            if (randomBoolean()) {
                action.alias(randomAlphaOfLength(5));
            } else {
                int numAliases = randomIntBetween(1, 5);
                String[] aliases = new String[numAliases];
                for (int i = 0; i < numAliases; i++) {
                    aliases[i] = "alias-" + randomAlphaOfLengthBetween(2, 5).toLowerCase(Locale.ROOT);
                }
                action.aliases(aliases);
            }
        }
        if (action.actionType() == AliasActions.Type.ADD) {
            if (randomBoolean()) {
                if (useStringAsFilter) {
                    action.filter(randomAlphaOfLength(5));
                } else {
                    action.filter(randomMap(randomInt(5)));
                }
            }
            if (randomBoolean()) {
                if (randomBoolean()) {
                    action.routing(randomRouting().toString());
                } else {
                    action.searchRouting(randomRouting().toString());
                    action.indexRouting(randomRouting().toString());
                }
            }
            if (randomBoolean()) {
                action.writeIndex(randomBoolean());
            }
            if (randomBoolean()) {
                action.isHidden(randomBoolean());
            }
        }
        return action;
    }

    public static Map<String, Object> randomMap(int maxDepth) {
        int members = between(0, 5);
        Map<String, Object> result = new HashMap<>(members);
        for (int i = 0; i < members; i++) {
            Object value;
            switch (between(0, 3)) {
            case 0:
                if (maxDepth > 0) {
                    value = randomMap(maxDepth - 1);
                } else {
                    value = randomAlphaOfLength(5);
                }
                break;
            case 1:
                value = randomAlphaOfLength(5);
                break;
            case 2:
                value = randomBoolean();
                break;
            case 3:
                value = randomLong();
                break;
            default:
                throw new UnsupportedOperationException();
            }
            result.put(randomAlphaOfLength(5), value);
        }
        return result;
    }

    public static Object randomRouting() {
        return randomBoolean() ? randomAlphaOfLength(5) : randomInt();
    }
}
