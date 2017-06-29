/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.tree;

public final class Location {
    private final int line;
    private final int charPositionInLine;

    public static final Location EMPTY = new Location(-1, -2);

    public Location(int line, int charPositionInLine) {
        this.line = line;
        this.charPositionInLine = charPositionInLine;
    }

    public int getLineNumber() {
        return line;
    }

    public int getColumnNumber() {
        return charPositionInLine + 1;
    }

    @Override
    public String toString() {
        return "@" + getLineNumber() + ":" + getColumnNumber();
    }
}
