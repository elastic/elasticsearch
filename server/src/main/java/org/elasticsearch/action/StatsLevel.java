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
    CLUSTER,
    NODE,
    INDICES,
    SHARDS;

    public static ActionRequestValidationException genIllegalClusterLevelException(String level) {
        String errorMsg = Strings.format(
            "level parameter must be one of [%s] or [%s] or [%s] but was [%s]",
            Strings.toLowercaseAscii(StatsLevel.CLUSTER.name()),
            Strings.toLowercaseAscii(StatsLevel.INDICES.name()),
            Strings.toLowercaseAscii(StatsLevel.SHARDS.name()),
            Strings.toLowercaseAscii(level)
        );
        return addValidationError(errorMsg, null);
    }

    public static ActionRequestValidationException genIllegalNodeLevelException(String level) {
        String errorMsg = Strings.format(
            "level parameter must be one of [%s] or [%s] or [%s] but was [%s]",
            Strings.toLowercaseAscii(StatsLevel.NODE.name()),
            Strings.toLowercaseAscii(StatsLevel.INDICES.name()),
            Strings.toLowercaseAscii(StatsLevel.SHARDS.name()),
            Strings.toLowercaseAscii(level)
        );
        return addValidationError(errorMsg, null);
    }
}
