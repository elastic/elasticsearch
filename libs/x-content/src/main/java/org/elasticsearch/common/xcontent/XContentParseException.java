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

import org.elasticsearch.common.Nullable;

import java.util.Optional;

/**
 * Thrown when one of the XContent parsers cannot parse something.
 */
public class XContentParseException extends IllegalArgumentException {

    private final Optional<XContentLocation> location;

    public XContentParseException(String message) {
        this(null, message);
    }

    public XContentParseException(XContentLocation location, String message) {
        super(message);
        this.location = Optional.ofNullable(location);
    }

    public XContentParseException(XContentLocation location, String message, Exception cause) {
        super(message, cause);
        this.location = Optional.ofNullable(location);
    }

    public int getLineNumber() {
        return location.map(l -> l.lineNumber).orElse(-1);
    }

    public int getColumnNumber() {
        return location.map(l -> l.columnNumber).orElse(-1);
    }

    @Nullable
    public XContentLocation getLocation() {
        return location.orElse(null);
    }

    @Override
    public String getMessage() {
        return location.map(l -> "[" + l.toString() + "] ").orElse("") + super.getMessage();
    }

}
