/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;
import java.util.Objects;

public class SourceToParse {

    private final BytesReference source;

    private final String id;

    private final @Nullable String routing;

    private final XContentType xContentType;

    private final Map<String, String> dynamicTemplates;

    private final boolean includeSourceOnError;

    private final XContentMeteringParserDecorator meteringParserDecorator;

    public SourceToParse(
        @Nullable String id,
        BytesReference source,
        XContentType xContentType,
        @Nullable String routing,
        Map<String, String> dynamicTemplates,
        boolean includeSourceOnError,
        XContentMeteringParserDecorator meteringParserDecorator
    ) {
        this.id = id;
        // we always convert back to byte array, since we store it and Field only supports bytes..
        // so, we might as well do it here, and improve the performance of working with direct byte arrays
        this.source = source.hasArray() ? source : new BytesArray(source.toBytesRef());
        this.xContentType = Objects.requireNonNull(xContentType);
        this.routing = routing;
        this.dynamicTemplates = Objects.requireNonNull(dynamicTemplates);
        this.includeSourceOnError = includeSourceOnError;
        this.meteringParserDecorator = meteringParserDecorator;
    }

    public SourceToParse(String id, BytesReference source, XContentType xContentType) {
        this(id, source, xContentType, null, Map.of(), true, XContentMeteringParserDecorator.NOOP);
    }

    public SourceToParse(String id, BytesReference source, XContentType xContentType, String routing) {
        this(id, source, xContentType, routing, Map.of(), true, XContentMeteringParserDecorator.NOOP);
    }

    public SourceToParse(
        String id,
        BytesReference source,
        XContentType xContentType,
        String routing,
        Map<String, String> dynamicTemplates
    ) {
        this(id, source, xContentType, routing, dynamicTemplates, true, XContentMeteringParserDecorator.NOOP);
    }

    public BytesReference source() {
        return this.source;
    }

    /**
     * The {@code _id} provided on the request or calculated on the
     * coordinating node. If the index is in {@code time_series} mode then
     * the coordinating node will not calculate the {@code _id}. In that
     * case this will be {@code null} if one isn't sent on the request.
     * <p>
     * Use {@link DocumentParserContext#documentDescription()} to generate
     * a description of the document for errors instead of calling this
     * method.
     */
    @Nullable
    public String id() {
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

    public XContentMeteringParserDecorator getMeteringParserDecorator() {
        return meteringParserDecorator;
    }

    public boolean getIncludeSourceOnError() {
        return includeSourceOnError;
    }
}
