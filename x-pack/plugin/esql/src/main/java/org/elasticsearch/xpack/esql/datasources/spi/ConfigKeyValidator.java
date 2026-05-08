/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/** Reject configuration keys not in any of the supplied claimed-key sets. */
public final class ConfigKeyValidator {

    private ConfigKeyValidator() {}

    /**
     * Throws {@link IllegalArgumentException} listing the unknown keys (sorted) and the recognised
     * options (sorted union of all {@code claimed} sets). No-op for null or empty {@code config}.
     */
    public static void check(Map<String, Object> config, Collection<Set<String>> claimed) {
        if (config == null || config.isEmpty()) {
            return;
        }
        List<String> unknown = null;
        for (String key : config.keySet()) {
            boolean recognised = false;
            for (Set<String> set : claimed) {
                if (set.contains(key)) {
                    recognised = true;
                    break;
                }
            }
            if (recognised == false) {
                if (unknown == null) {
                    unknown = new ArrayList<>();
                }
                unknown.add(key);
            }
        }
        if (unknown == null) {
            return;
        }
        Set<String> allRecognised = new TreeSet<>();
        for (Set<String> set : claimed) {
            allRecognised.addAll(set);
        }
        unknown.sort(String::compareTo);
        StringBuilder message = new StringBuilder("unknown option").append(unknown.size() == 1 ? "" : "s")
            .append(" ")
            .append(unknown)
            .append(" in data source configuration");
        if (allRecognised.isEmpty()) {
            message.append("; no options are recognised in this context");
        } else {
            message.append("; recognised options are ").append(allRecognised);
        }
        throw new IllegalArgumentException(message.toString());
    }
}
