/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

/**
 * A filter that filter fields away from source
 */
public interface XContentFieldFilter {
    /**
     * filter source in {@link BytesReference} format and in {@link XContentType} content type
     */
    BytesReference apply(BytesReference sourceBytes, XContentType xContentType) throws IOException;
}
