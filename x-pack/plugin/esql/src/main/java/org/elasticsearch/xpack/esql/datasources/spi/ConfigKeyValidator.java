/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.xpack.esql.datasources.DatasetRewriter;

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
     * Keys the framework injects into the config map itself, which no user ever typed. The dataset layer wraps a
     * dataset's parent data-source settings under {@code _datasource} before handing the map to a factory — but only
     * when the parent actually has settings (see {@link DatasetRewriter}{@code #mergeSettings}).
     * <p>
     * They are excluded from the unknown-key report because this check exists to tell a user which of <em>their</em>
     * settings was not understood. A factory that claims no per-query options at all (the table catalogs pass an empty
     * claimed-set) would otherwise report {@code unknown option [_datasource] in data source configuration; no options
     * are recognised in this context} for a perfectly ordinary dataset — naming a key the user cannot remove, and
     * describing a configuration problem where the real one is that the format could not be resolved.
     * <p>
     * Matched by the leading underscore rather than by an explicit list, so a framework key added later is covered
     * without having to remember this file. The underscore prefix is already the convention for these
     * ({@code ExternalSourceResolver#DATASOURCE_CONFIG_KEY}), and no user-facing setting uses it.
     */
    private static boolean isFrameworkKey(String key) {
        return key.startsWith("_");
    }

    /**
     * Throws {@link IllegalArgumentException} listing the unknown keys (sorted) and the recognised
     * options (sorted union of all {@code claimed} sets). No-op for null or empty {@code config}.
     * Framework-injected keys (see {@link #isFrameworkKey}) are never reported.
     */
    public static void check(Map<String, Object> config, Collection<Set<String>> claimed) {
        if (config == null || config.isEmpty()) {
            return;
        }
        List<String> unknown = null;
        for (String key : config.keySet()) {
            if (isFrameworkKey(key)) {
                continue;
            }
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
