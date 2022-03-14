/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;
import java.util.Objects;

public class SourceToParse {

    private final BytesReference source;

    private final String id;

    private final @Nullable String routing;

    private final XContentType xContentType;

    private final Map<String, String> dynamicTemplates;

    public SourceToParse(
        @Nullable String id,
        BytesReference source,
        XContentType xContentType,
        @Nullable String routing,
        Map<String, String> dynamicTemplates
    ) {
        this.id = id;
        // we always convert back to byte array, since we store it and Field only supports bytes..
        // so, we might as well do it here, and improve the performance of working with direct byte arrays
        this.source = new BytesArray(Objects.requireNonNull(source).toBytesRef());
        this.xContentType = Objects.requireNonNull(xContentType);
        this.routing = routing;
        this.dynamicTemplates = Objects.requireNonNull(dynamicTemplates);
    }

    public SourceToParse(String id, BytesReference source, XContentType xContentType) {
        this(id, source, xContentType, null, Map.of());
    }

    public BytesReference source() {
        return this.source;
    }

    public String id() { // TODO migrate callers that use this to describe the document to a new method
        return this.id;
    }

    public @Nullable String routing() {
        return this.routing;
    }

    /**
     * Returns a map from the full path (i.e. foo.bar) of field names to the names of dynamic mapping templates.
     */
    public Map<String, String> dynamicTemplates() {
        return dynamicTemplates;
    }

    public XContentType getXContentType() {
        return this.xContentType;
    }
}
