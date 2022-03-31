/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.proto.content;

/**
 * Light clone of XContentLocation
 */
public class ContentLocation {

    public static final ContentLocation UNKNOWN = new ContentLocation(-1, -1);

    public final int lineNumber;
    public final int columnNumber;

    public ContentLocation(int lineNumber, int columnNumber) {
        super();
        this.lineNumber = lineNumber;
        this.columnNumber = columnNumber;
    }

    @Override
    public String toString() {
        return lineNumber + ":" + columnNumber;
    }
}
