/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.xcontent.XContentType;

import java.util.Map;
import java.util.Objects;

/**
 * Describes one document for {@link DocumentMapper#parse(SourceToParse)} to parse: its source, in
 * either {@link DocumentSource} form, plus the request metadata parsing needs — id, routing, dynamic
 * template choices, the metering decorator, and the time-series id when the coordinating node
 * computed one during routing.
 */
public class SourceToParse {

    private final String id;

    private final @Nullable BytesRef tsid;

    private final @Nullable String routing;

    private final Map<String, String> dynamicTemplates;

    private final Map<String, Map<String, String>> dynamicTemplateParams;

    private final DocumentSource source;

    private final XContentMeteringParserDecorator meteringParserDecorator;

    public SourceToParse(
        @Nullable String id,
        DocumentSource source,
        @Nullable String routing,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams,
        XContentMeteringParserDecorator meteringParserDecorator,
        @Nullable BytesRef tsid
    ) {
        this.id = id;
        this.source = Objects.requireNonNull(source);
        this.routing = routing;
        this.dynamicTemplates = Objects.requireNonNull(dynamicTemplates);
        this.dynamicTemplateParams = dynamicTemplateParams;
        this.meteringParserDecorator = meteringParserDecorator;
        this.tsid = tsid;
    }

    public SourceToParse(String id, BytesReference source, XContentType xContentType) {
        this(id, source, xContentType, null);
    }

    public SourceToParse(String id, BytesReference source, XContentType xContentType, @Nullable String routing) {
        this(id, new BytesSource(source, xContentType, true), routing, Map.of(), Map.of(), XContentMeteringParserDecorator.NOOP, null);
    }

    public DocumentSource source() {
        return source;
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

    public Map<String, Map<String, String>> dynamicTemplateParams() {
        return dynamicTemplateParams;
    }

    public XContentMeteringParserDecorator getMeteringParserDecorator() {
        return meteringParserDecorator;
    }

    public BytesRef tsid() {
        return tsid;
    }
}
