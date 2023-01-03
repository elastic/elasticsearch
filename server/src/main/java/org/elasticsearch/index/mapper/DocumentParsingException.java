/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentParseException;

public class DocumentParsingException extends XContentParseException {

    public DocumentParsingException(XContentLocation location, String message) {
        super(location, message);
    }

    public DocumentParsingException(XContentLocation location, String message, Exception cause) {
        super(location, message, cause);
    }
}
