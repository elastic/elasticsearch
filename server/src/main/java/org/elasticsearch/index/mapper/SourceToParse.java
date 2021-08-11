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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.Nullable;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class SourceToParse {
    /**
     * Create a Function that will return a {@link SourceToParse} that parses
     * the {@link #timeSeriesId()} from the {@code _source} of the document
     * once the {@link DocumentMapper} has been resolved.
     */
    public static Function<DocumentMapper, SourceToParse> parseTimeSeriesIdFromSource(
        String index,
        String id,
        BytesReference source,
        XContentType xContentType,
        @Nullable String routing,
        Map<String, String> dynamicTemplates
    ) {
        return documentMapper -> parseTimeSeriesIdFromSource(
            index,
            id,
            source,
            xContentType,
            routing,
            dynamicTemplates,
            documentMapper.mappers()
        );
    }

    /**
     * Create a {@link SourceToParse} that parses the {@link #timeSeriesId()} from
     * the {@code _source}.
     */
    public static SourceToParse parseTimeSeriesIdFromSource(
        String index,
        String id,
        BytesReference source,
        XContentType xContentType,
        @Nullable String routing,
        Map<String, String> dynamicTemplates,
        MappingLookup lookup
    ) {
        return new SourceToParse(
            index,
            id,
            source,
            xContentType,
            routing,
            lookup.getMapping().generateTimeSeriesIdIfNeeded(source, xContentType),
            dynamicTemplates
        );
    }

    private final BytesReference source;

    private final String index;

    private final String id;

    private final @Nullable String routing;

    private final @Nullable BytesReference timeSeriesId;

    private final XContentType xContentType;

    private final Map<String, String> dynamicTemplates;

    public SourceToParse(String index, String id, BytesReference source, XContentType xContentType, @Nullable String routing,
                         @Nullable BytesReference timeSeriesId, Map<String, String> dynamicTemplates) {
        if (routing != null && timeSeriesId != null) {
            throw new IllegalArgumentException(
                "only one of routing or timeSeriesId are supported but got [" + routing + "] and " + timeSeriesId.toBytesRef()
            );
        }
        this.index = Objects.requireNonNull(index);
        this.id = Objects.requireNonNull(id);
        // we always convert back to byte array, since we store it and Field only supports bytes..
        // so, we might as well do it here, and improve the performance of working with direct byte arrays
        this.source = new BytesArray(Objects.requireNonNull(source).toBytesRef());
        this.xContentType = Objects.requireNonNull(xContentType);
        this.routing = routing;
        this.timeSeriesId = timeSeriesId;
        this.dynamicTemplates = Objects.requireNonNull(dynamicTemplates);
    }

    public SourceToParse(String index, String id, BytesReference source, XContentType xContentType) {
        this(index, id, source, xContentType, null, null, Map.of());
    }

    public BytesReference source() {
        return this.source;
    }

    public String index() {
        return this.index;
    }

    public String id() {
        return this.id;
    }

    public @Nullable String routing() {
        return this.routing;
    }

    public @Nullable BytesReference timeSeriesId() {
        return this.timeSeriesId;
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

    public enum Origin {
        PRIMARY,
        REPLICA
    }
}
