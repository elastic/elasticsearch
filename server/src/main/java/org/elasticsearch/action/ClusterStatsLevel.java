/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.xcontent.ToXContent;

public enum ClusterStatsLevel {
    CLUSTER("cluster"),
    INDICES("indices"),
    SHARDS("shards");

    private final String level;

    ClusterStatsLevel(String level) {
        this.level = level;
    }

    public String getLevel() {
        return level;
    }

    public static ClusterStatsLevel of(String level) {
        for (ClusterStatsLevel value : values()) {
            if (value.getLevel().equalsIgnoreCase(level)) {
                return value;
            }
        }
        throw new IllegalArgumentException("level parameter must be one of [cluster] or [indices] or [shards] but was [" + level + "]");
    }

    public static ClusterStatsLevel of(ToXContent.Params params, ClusterStatsLevel defaultLevel) {
        return ClusterStatsLevel.of(params.param("level", defaultLevel.getLevel()));
    }
}
