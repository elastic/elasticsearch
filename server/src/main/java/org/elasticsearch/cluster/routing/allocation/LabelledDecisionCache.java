/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.routing.allocation.decider.Decision;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/// When a decider returns a [Decision.Type#NO] or [Decision.Type#NOT_PREFERRED] decision, this cache
/// stores the decision with the label populated. Useful for tracking metrics.
///
/// Some safety measures are in place to prevent the cache from growing too large.
public class LabelledDecisionCache {

    private static final int DECIDER_SIZE_LIMIT = 500;
    private static final Decision.Type[] INTERESTING_DECISION_TYPES = new Decision.Type[] { Decision.Type.NO, Decision.Type.NOT_PREFERRED };
    private final EnumMap<Decision.Type, Map<String, Decision>> decisionCache = new EnumMap<>(Decision.Type.class);

    public LabelledDecisionCache() {
        for (Decision.Type type : INTERESTING_DECISION_TYPES) {
            decisionCache.put(type, new ConcurrentHashMap<>());
        }
    }

    public Decision get(Decision.Type type, String label) {
        assert cacheIsNotGrowingUnreasonably() : "Decision cache is growing beyond expectations, please investigate";
        return decisionCache.get(type).computeIfAbsent(label, k -> new Decision.Single(type, k, null));
    }

    private boolean cacheIsNotGrowingUnreasonably() {
        for (Map<String, Decision> typeCache : decisionCache.values()) {
            if (typeCache.size() > DECIDER_SIZE_LIMIT) {
                return false;
            }
        }
        return true;
    }
}
