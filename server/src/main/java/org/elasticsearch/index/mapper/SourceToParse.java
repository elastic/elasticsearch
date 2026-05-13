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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.eirf.EirfRowReader;
import org.elasticsearch.eirf.EirfRowToXContent;
import org.elasticsearch.eirf.EirfRowXContentParser;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Objects;

public class SourceToParse {

    private final String id;

    private final @Nullable BytesRef tsid;

    private final @Nullable String routing;

    private final Map<String, String> dynamicTemplates;

    private final Map<String, Map<String, String>> dynamicTemplateParams;

    private final Source source;

    private final XContentMeteringParserDecorator meteringParserDecorator;

    public SourceToParse(
        @Nullable String id,
        BytesReference source,
        XContentType xContentType,
        @Nullable String routing,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams,
        boolean includeSourceOnError,
        XContentMeteringParserDecorator meteringParserDecorator,
        @Nullable BytesRef tsid
    ) {
        this(
            id,
            source,
            xContentType,
            routing,
            dynamicTemplates,
            dynamicTemplateParams,
            includeSourceOnError,
            meteringParserDecorator,
            tsid,
            null,
            null
        );
    }

    public SourceToParse(
        @Nullable String id,
        EirfRowXContentParser.SchemaNode schemaTree,
        EirfRowReader row,
        XContentType xContentType,
        @Nullable String routing,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams,
        boolean includeSourceOnError,
        XContentMeteringParserDecorator meteringParserDecorator,
        @Nullable BytesRef tsid
    ) {
        this(
            id,
            null,
            xContentType,
            routing,
            dynamicTemplates,
            dynamicTemplateParams,
            includeSourceOnError,
            meteringParserDecorator,
            tsid,
            schemaTree,
            row
        );
    }

    private SourceToParse(
        @Nullable String id,
        @Nullable BytesReference source,
        XContentType xContentType,
        @Nullable String routing,
        Map<String, String> dynamicTemplates,
        Map<String, Map<String, String>> dynamicTemplateParams,
        boolean includeSourceOnError,
        XContentMeteringParserDecorator meteringParserDecorator,
        @Nullable BytesRef tsid,
        @Nullable EirfRowXContentParser.SchemaNode schemaTree,
        @Nullable EirfRowReader row
    ) {
        this.id = id;
        this.routing = routing;
        this.dynamicTemplates = Objects.requireNonNull(dynamicTemplates);
        this.dynamicTemplateParams = dynamicTemplateParams;
        this.meteringParserDecorator = meteringParserDecorator;
        this.tsid = tsid;
        this.source = new Source(schemaTree, row, source, xContentType, includeSourceOnError);
    }

    public SourceToParse(String id, BytesReference source, XContentType xContentType) {
        this(id, source, xContentType, null, Map.of(), Map.of(), true, XContentMeteringParserDecorator.NOOP, null);
    }

    public SourceToParse(String id, BytesReference source, XContentType xContentType, String routing) {
        this(id, source, xContentType, routing, Map.of(), Map.of(), true, XContentMeteringParserDecorator.NOOP, null);
    }

    public SourceToParse(
        String id,
        BytesReference source,
        XContentType xContentType,
        String routing,
        Map<String, String> dynamicTemplates,
        BytesRef tsid
    ) {
        this(id, source, xContentType, routing, dynamicTemplates, Map.of(), true, XContentMeteringParserDecorator.NOOP, tsid);
    }

    public Source source() {
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

    public XContentParser getParser(XContentParserConfiguration configuration) throws IOException {
        return source.parser(configuration);
    }

    // TODO: Eventually will want to combine this with our other source abstractions IndexSource, etc.
    public static class Source {

        private final boolean includeSourceOnError;
        private final EirfRowXContentParser.SchemaNode schemaTree;
        private final EirfRowReader row;
        private final XContentType xContentType;
        private BytesReference originalSourceBytes;

        private Source(
            EirfRowXContentParser.SchemaNode schemaTree,
            EirfRowReader row,
            BytesReference originalSourceBytes,
            XContentType xContentType,
            boolean includeSourceOnError
        ) {
            // originalSourceBytes must be null if row is not null. And vice versa.
            assert originalSourceBytes == null || row == null;
            this.schemaTree = schemaTree;
            this.row = row;
            // we always convert back to byte array, since we store it and Field only supports bytes.
            // so, we might as well do it here, and improve the performance of working with direct byte arrays.
            this.originalSourceBytes = originalSourceBytes == null ? null
                : originalSourceBytes.hasArray() ? originalSourceBytes
                : new BytesArray(originalSourceBytes.toBytesRef());
            this.xContentType = Objects.requireNonNull(xContentType);
            this.includeSourceOnError = includeSourceOnError;
        }

        public boolean isEmpty() {
            return (row != null && row.columnCount() == 0)
                || (row == null && (originalSourceBytes == null || originalSourceBytes.length() == 0));
        }

        public XContentType xContentType() {
            return xContentType;
        }

        public XContentParser parser(XContentParserConfiguration configuration) throws IOException {
            if (row != null) {
                // TODO: EIRF does not current support XContentParserConfiguration or includeSourceOnError. Need to evaluate these features.
                return new EirfRowXContentParser(schemaTree, row);
            } else {
                return XContentHelper.createParser(
                    configuration.withIncludeSourceOnError(includeSourceOnError),
                    originalSourceBytes,
                    xContentType
                );
            }
        }

        // Synchronized for now to be safe. Probably unnecessary.
        public synchronized BytesReference originalBytes() {
            if (originalSourceBytes == null) {
                try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
                    EirfRowToXContent.writeRowFromSchema(row, schemaTree, builder);
                    originalSourceBytes = BytesReference.bytes(builder);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return originalSourceBytes;
        }
    }
}
