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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/// When a decider returns a decision of one of the types in [#INTERESTING_DECISION_TYPES], this cache
/// stores the decision with the label populated. Useful for tracking metrics.
///
/// Some safety measures are in place to prevent the cache from growing too large.
public class LabelledDecisionCache {

    private static final Logger logger = LogManager.getLogger(LabelledDecisionCache.class);

    /// This is a hard limit on how many decisions we'll cache per decision type, we shouldn't hit it if things are working as expected.
    static final int DECIDER_SIZE_LIMIT = 500;
    private static final Set<Decision.Type> INTERESTING_DECISION_TYPES = Set.of(Decision.Type.NO, Decision.Type.NOT_PREFERRED);
    private final EnumMap<Decision.Type, Map<String, Decision>> decisionCache = new EnumMap<>(Decision.Type.class);
    /// Ensures we only warn the first time any of the caches fills up
    private final AtomicBoolean fullWarningLogged = new AtomicBoolean();

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
                maybeLogCacheFilledWarning(type);
                return stringDecisionMap.getOrDefault(label, decision);
            }
            return stringDecisionMap.computeIfAbsent(label, k -> new Decision.Single(type, k, null));
        }
        return decision;
    }

    /// Log a warning the first time any cache fills up. This should help to troubleshoot if it ever occurs in a live
    /// environment
    private void maybeLogCacheFilledWarning(Decision.Type type) {
        if (fullWarningLogged.compareAndSet(false, true)) {
            logger.warn(
                "labelled decision cache for [{}] decisions is full at [{}] entries, "
                    + "further decisions with new labels will be unlabelled",
                type,
                DECIDER_SIZE_LIMIT
            );
        }
    }
}
