/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.common.Strings;

import java.io.IOException;

public class RequestedRangeNotSatisfiedException extends IOException {

    private final String resource;
    private final long position;
    private final long length;

    public RequestedRangeNotSatisfiedException(String resource, long position, long length) {
        super(message(resource, position, length));
        this.resource = resource;
        this.position = position;
        this.length = length;
    }

    public RequestedRangeNotSatisfiedException(String resource, long position, long length, Throwable cause) {
        super(message(resource, position, length), cause);
        this.resource = resource;
        this.position = position;
        this.length = length;
    }

    public String getResource() {
        return resource;
    }

    public long getPosition() {
        return position;
    }

    public long getLength() {
        return length;
    }

    private static String message(String resource, long position, long length) {
        return Strings.format("Requested range [position=%d, length=%d] cannot be satisfied for [%s]", position, length, resource);
    }
}
