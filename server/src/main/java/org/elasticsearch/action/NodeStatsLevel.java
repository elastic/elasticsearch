/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.xcontent.ToXContent;

public enum NodeStatsLevel {
    NODE("node"),
    INDICES("indices"),
    SHARDS("shards");

    private final String level;

    NodeStatsLevel(String level) {
        this.level = level;
    }

    public String getLevel() {
        return level;
    }

    public static NodeStatsLevel of(String level) {
        for (NodeStatsLevel value : values()) {
            if (value.getLevel().equalsIgnoreCase(level)) {
                return value;
            }
        }
        throw new IllegalArgumentException("level parameter must be one of [node] or [indices] or [shards] but was [" + level + "]");
    }

    public static NodeStatsLevel of(ToXContent.Params params, NodeStatsLevel defaultLevel) {
        return NodeStatsLevel.of(params.param("level", defaultLevel.getLevel()));
    }
}
