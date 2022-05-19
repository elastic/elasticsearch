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
 * Write operation for documents
 */
public enum Op { // This is a clone of org.elasticsearch.action.DocWriteRequest.OpType
    NOOP("noop"),
    INDEX("index"),
    DELETE("delete"),
    CREATE("create"),
    UNKOWN("unknown");

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
            return UNKOWN;
        }
        return switch (op.toLowerCase(Locale.ROOT)) {
            case "noop" -> NOOP;
            case "index" -> INDEX;
            case "delete" -> DELETE;
            case "create" -> CREATE;
            default -> UNKOWN;
        };
    }

    @Override
    public String toString() {
        return name;
    }
}
