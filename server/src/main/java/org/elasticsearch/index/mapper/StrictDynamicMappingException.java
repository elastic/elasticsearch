/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.XContentLocation;

import java.io.IOException;

public class StrictDynamicMappingException extends DocumentParsingException {

    public StrictDynamicMappingException(XContentLocation location, String path, String fieldName) {
        super(location, "mapping set to strict, dynamic introduction of [" + fieldName + "] within [" + path + "] is not allowed");
    }

    public StrictDynamicMappingException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
