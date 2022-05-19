/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing;

/**
 * Routing Preference Type
 */
public enum Preference {

    /**
     * Route to specific shards
     */
    SHARDS("_shards"),

    /**
     * Route to preferred nodes, if possible
     */
    PREFER_NODES("_prefer_nodes"),

    /**
     * Route to local node, if possible
     */
    LOCAL("_local"),

    /**
     * Route to the local shard only
     */
    ONLY_LOCAL("_only_local"),

    /**
     * Route to only node with attribute
     */
    ONLY_NODES("_only_nodes");

    private final String type;

    Preference(String type) {
        this.type = type;
    }

    public String type() {
        return type;
    }

    /**
     * Parses the Preference Type given a string
     */
    public static Preference parse(String preference) {
        String preferenceType;
        int colonIndex = preference.indexOf(':');
        if (colonIndex == -1) {
            preferenceType = preference;
        } else {
            preferenceType = preference.substring(0, colonIndex);
        }

        return switch (preferenceType) {
            case "_shards" -> SHARDS;
            case "_prefer_nodes" -> PREFER_NODES;
            case "_local" -> LOCAL;
            case "_only_local", "_onlyLocal" -> ONLY_LOCAL;
            case "_only_nodes" -> ONLY_NODES;
            default -> throw new IllegalArgumentException("no Preference for [" + preferenceType + "]");
        };
    }

}
