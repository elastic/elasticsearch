/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentLocation;

import java.io.IOException;

/**
 * An exception thrown during document parsing
 *
 * Contains information about the location in the document where the error was encountered
 */
public class DocumentParsingException extends ElasticsearchException {

    public DocumentParsingException(XContentLocation location, String message) {
        super(message(location, message));
    }

    public DocumentParsingException(XContentLocation location, String message, Exception cause) {
        super(message(location, message), cause);
    }

    public DocumentParsingException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }

    private static String message(XContentLocation location, String message) {
        return location == XContentLocation.UNKNOWN ? message : "[" + location + "] " + message;
    }
}
