/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.sourcebatch.SourceRow;
import org.elasticsearch.sourcebatch.SourceRowToXContent;
import org.elasticsearch.sourcebatch.SourceRowXContentParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Objects;

/**
 * Holds a document as one row of a column-major batch, together with the schema tree that every row
 * of that batch shares, so batch indexing reads documents straight out of the batch. Parsing walks
 * the schema and pulls values from the row, and the x-content bytes exist only once a caller asks
 * for them.
 *
 * <p>Borrows the row: it reads from the batch's buffers, so it stays valid while the batch stays open.
 */
public final class RowSource implements DocumentSource {

    private final SourceRowXContentParser.SchemaNode schemaTree;
    private final SourceRow row;
    private final XContentType xContentType;
    private BytesReference materializedBytes;

    /**
     * Wraps {@code row} for reading through {@code schemaTree}, which must be built from the schema of
     * the batch {@code row} belongs to, since the tree addresses that batch's columns by index.
     */
    public RowSource(SourceRowXContentParser.SchemaNode schemaTree, SourceRow row, XContentType xContentType) {
        this.schemaTree = Objects.requireNonNull(schemaTree);
        this.row = Objects.requireNonNull(row);
        this.xContentType = Objects.requireNonNull(xContentType);
    }

    @Override
    public XContentType xContentType() {
        return xContentType;
    }

    /**
     * Returns {@code true}: the encoder produces a row only from a top-level object, so every row
     * stands for a document that arrived with content, including one with no fields.
     */
    @Override
    public boolean hasContent() {
        return true;
    }

    @Override
    public int estimatedSizeInBytes() {
        // TODO: Consider including the size of the schema
        return row.sizeInBytes();
    }

    /**
     * Streams every field of the row as is; {@code configuration}, including its filtering and
     * include-source-on-error setting, leaves the output unchanged.
     */
    @Override
    public XContentParser parser(XContentParserConfiguration configuration) {
        // TODO: batch row parsing does not currently support XContentParserConfiguration or includeSourceOnError. Need to evaluate
        // these features.
        return new SourceRowXContentParser(schemaTree, row);
    }

    // Synchronized for now to be safe. Probably unnecessary.
    @Override
    public synchronized BytesReference originalBytes() {
        if (materializedBytes == null) {
            try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
                SourceRowToXContent.writeRowFromSchema(row, schemaTree, builder);
                materializedBytes = BytesReference.bytes(builder);
            } catch (IOException e) {
                assert false : e.getMessage();
                throw new UncheckedIOException(e);
            }
        }
        return materializedBytes;
    }
}
