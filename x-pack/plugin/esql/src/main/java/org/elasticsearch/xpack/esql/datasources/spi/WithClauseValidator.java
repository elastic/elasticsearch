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

/**
 * Generic WITH-clause validator. Each {@link ExternalSourceFactory} delegates config processing
 * to one or more sub-components (file: storage backend + format reader; future: catalog +
 * connection; etc.). Each sub-component returns a {@link Configured} carrying the keys it
 * consumed. The factory hands those consumed sets to {@link #check} and any leftover key in the
 * input map produces an {@link IllegalArgumentException} naming the unknown keys plus the union
 * of recognised options.
 * <p>
 * The contract is intentionally minimal: pass any number of claimed-key sets, get a deterministic
 * error on a leftover. Provider-specific composition (which layers, which static cross-cutting
 * keys) lives in each factory's own {@link ExternalSourceFactory#validateConfig override}.
 */
public final class WithClauseValidator {

    private WithClauseValidator() {}

    /**
     * Rejects any key in {@code config} that is not in any of the {@code claimed} sets.
     * <p>
     * On rejection, throws {@link IllegalArgumentException} listing the unknown keys (sorted, for
     * deterministic messages) and the union of all recognised options (also sorted). Matches
     * "unknown option X" / "unknown options [X, Y]" wording so callers don't need to format twice.
     *
     * @param config  the WITH-clause map; {@code null} or empty is a no-op
     * @param claimed each layer's consumed-key set; pass an empty list to reject any non-empty config
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
        throw new IllegalArgumentException(
            "unknown option" + (unknown.size() == 1 ? "" : "s") + " " + unknown + " in WITH clause; recognised options are " + allRecognised
        );
    }
}
