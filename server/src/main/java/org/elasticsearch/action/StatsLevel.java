/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action;

import org.elasticsearch.common.Strings;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public enum StatsLevel {
    CLUSTER("cluster"),
    NODE("node"),
    INDICES("indices"),
    SHARDS("shards");

    private final String name;

    StatsLevel(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public static ActionRequestValidationException clusterLevelsValidation(final String level) {
        final boolean isLevelValid = CLUSTER.getName().equalsIgnoreCase(level)
            || INDICES.getName().equalsIgnoreCase(level)
            || SHARDS.getName().equalsIgnoreCase(level);

        ActionRequestValidationException validationException = null;
        if (isLevelValid == false) {
            String errorMsg = Strings.format(
                "level parameter must be one of [%s] or [%s] or [%s] but was [%s]",
                CLUSTER.getName(),
                INDICES.getName(),
                SHARDS.getName(),
                level
            );
            validationException = addValidationError(errorMsg, null);
        }
        return validationException;
    }

    public static ActionRequestValidationException nodeLevelsValidation(final String level) {
        final boolean isLevelValid = NODE.getName().equalsIgnoreCase(level)
            || INDICES.getName().equalsIgnoreCase(level)
            || SHARDS.getName().equalsIgnoreCase(level);

        ActionRequestValidationException validationException = null;
        if (isLevelValid == false) {
            String errorMsg = Strings.format(
                "level parameter must be one of [%s] or [%s] or [%s] but was [%s]",
                NODE.getName(),
                INDICES.getName(),
                SHARDS.getName(),
                level
            );
            validationException = addValidationError(errorMsg, null);
        }
        return validationException;
    }
}
