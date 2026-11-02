/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A plan fragment that a satellite engine executes instead of ES|QL (see the satellite protocol spec, section 7). It rides in
 * {@code ExternalSourceExec#pushedFilter()}: opaque, never serialized, rebuilt on each node by the local physical optimizer.
 *
 * @param request the fragment request as a JSON-compatible tree (specVersion, format, settings, output, plan), without the
 *                {@code partitions} entry, which the connector adds per split
 */
public record SatelliteFragment(Map<String, Object> request) {

    /**
     * Key in {@code ExternalSourceExec#sourceMetadata()} (which is serialized, so it reaches every worker) under which the
     * capability declaration of the satellite travels, plus {@code listColumns}: the columns the satellite sends as Arrow lists.
     */
    public static final String CAPABILITY_KEY = "_satellite";

    /** The spec versions this ES speaks: it announces them in the capability request and uses the highest the satellite shares. */
    public static final List<Integer> SUPPORTED_SPEC_VERSIONS = List.of(0, 1);

    /**
     * Config keys of a satellite dataset that are not source options: what the validator derives from the resource and what the
     * dataset layer adds. Keys that start with an underscore are framework keys and are never options either.
     */
    public static final Set<String> NON_OPTION_KEYS = Set.of("endpoint", "target", "schema_resolution");

    /**
     * Whether the fragment filters rows before they leave the satellite. How many splits it takes to fill a LIMIT is then unknown,
     * so the number of drivers cannot be derived from the limit.
     */
    public boolean filtersRows() {
        Object node = request.get("plan");
        while (node instanceof Map<?, ?> n) {
            if ("filter".equals(n.get("op"))) {
                return true;
            }
            node = n.get("input");
        }
        return false;
    }

    public static boolean isOptionKey(String key) {
        return NON_OPTION_KEYS.contains(key) == false && key.startsWith("_") == false;
    }
}
