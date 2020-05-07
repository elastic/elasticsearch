/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.xcontent;

/**
 * Simple data structure representing the line and column number of a position
 * in some XContent e.g. JSON. Locations are typically used to communicate the
 * position of a parsing error to end users and consequently have line and
 * column numbers starting from 1.
 */
public final class XContentLocation {

    public static final XContentLocation UNKNOWN = new XContentLocation(-1, -1);

    public final int lineNumber;
    public final int columnNumber;

    public XContentLocation(int lineNumber, int columnNumber) {
        super();
        this.lineNumber = lineNumber;
        this.columnNumber = columnNumber;
    }

    @Override
    public String toString() {
        return lineNumber + ":" + columnNumber;
    }
}
