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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/// When a decider returns a decision of one of the types in [#INTERESTING_DECISION_TYPES], this cache
/// stores the decision with the label populated. Useful for tracking metrics.
///
/// Some safety measures are in place to prevent the cache from growing too large.
public class LabelledDecisionCache {

    /// This is a hard limit on how many decisions we'll cache per decision type, we shouldn't hit it if things are working as expected.
    private static final int DECIDER_SIZE_LIMIT = 500;
    private static final Set<Decision.Type> INTERESTING_DECISION_TYPES = Set.of(Decision.Type.NO, Decision.Type.NOT_PREFERRED);
    private final EnumMap<Decision.Type, Map<String, Decision>> decisionCache = new EnumMap<>(Decision.Type.class);

    public LabelledDecisionCache() {
        for (Decision.Type type : INTERESTING_DECISION_TYPES) {
            decisionCache.put(type, new ConcurrentHashMap<>());
        }
    }

    public Decision get(Decision decision, String label) {
        final var type = decision.type();
        if (INTERESTING_DECISION_TYPES.contains(type) && label != null) {
            final var stringDecisionMap = decisionCache.get(type);
            if (stringDecisionMap.size() >= DECIDER_SIZE_LIMIT) {
                return stringDecisionMap.getOrDefault(label, decision);
            }
            return stringDecisionMap.computeIfAbsent(label, k -> new Decision.Single(type, k, null));
        }
        return decision;
    }
}
