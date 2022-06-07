/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import java.util.Locale;

/**
 * Write operation for documents.
 *
 * This is similar to {@link org.elasticsearch.action.DocWriteRequest.OpType} with NOOP and UNKNOWN added.  UPDATE
 */
public enum Op {
    NOOP("noop"),
    INDEX("index"),
    DELETE("delete"),
    CREATE("create"),
    UPDATE("update"),
    UNKNOWN("unknown");

    public final String name;

    Op(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /**
     * Get the appropriate OP for the given string.  Returns UNKNOWN for no matches
     * @param op - the string version of op.
     */
    public static Op fromString(String op) {
        if (op == null) {
            return UNKNOWN;
        }
        return switch (op.toLowerCase(Locale.ROOT)) {
            case "noop" -> NOOP;
            case "index" -> INDEX;
            case "delete" -> DELETE;
            case "create" -> CREATE;
            case "update" -> UPDATE;
            default -> UNKNOWN;
        };
    }

    @Override
    public String toString() {
        return name;
    }
}
