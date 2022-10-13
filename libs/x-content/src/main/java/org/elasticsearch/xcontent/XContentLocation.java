/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

/**
 * Simple data structure representing the line and column number of a position
 * in some XContent e.g. JSON. Locations are typically used to communicate the
 * position of a parsing error to end users and consequently have line and
 * column numbers starting from 1.
 */
public record XContentLocation(int lineNumber, int columnNumber) {

    public static final XContentLocation UNKNOWN = new XContentLocation(-1, -1);

    @Override
    public String toString() {
        return lineNumber + ":" + columnNumber;
    }
}
