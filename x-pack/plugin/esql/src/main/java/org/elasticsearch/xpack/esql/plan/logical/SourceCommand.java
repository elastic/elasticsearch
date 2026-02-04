/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical;

import org.elasticsearch.index.IndexMode;

/**
 * Contains all varieties of source commands (FROM, TS, PROMQL, ...) and their associated index mode.
 */
public enum SourceCommand {
    FROM(IndexMode.STANDARD),
    TS(IndexMode.TIME_SERIES),
    PROMQL(IndexMode.TIME_SERIES);

    private final IndexMode indexMode;

    SourceCommand(IndexMode indexMode) {
        this.indexMode = indexMode;
    }

    /**
     * Checks if the given command string corresponds to a valid SourceCommand.
     *
     * @param command the command string to check
     * @return true if the command is a valid SourceCommand, false otherwise
     */
    public static boolean isSourceCommand(String command) {
        for (SourceCommand cmd : values()) {
            if (cmd.name().equalsIgnoreCase(command)) {
                return true;
            }
        }
        return false;
    }

    public IndexMode indexMode() {
        return indexMode;
    }
}
