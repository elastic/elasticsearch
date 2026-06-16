/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.index.LeafReader;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper;
import org.elasticsearch.index.mapper.SourceLoader;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Wraps a mapped sub-field's {@link SourceLoader.SyntheticFieldLoader} so that, when its value is merged
 * into the flattened root, every scalar leaf is rendered as a JSON string.
 * <p>
 * The flattened root is "stringly typed": unmapped keyed values are always strings, and
 * {@link FlattenedSourceValueFetcher} stringifies every leaf it reads from {@code _source}. A mapped
 * sub-field, however, writes its native type through its own synthetic loader (a {@code long} sub-field
 * emits {@code 200}, not {@code "200"}). That makes loading the flattened root diverge between the
 * doc-values path (native) and the stored/source path (string). Coercing the merged sub-field values to
 * strings here keeps the flattened root identical regardless of which loading path the query takes.
 * <p>
 * This only affects loading the <em>root</em> as a flattened blob (e.g. {@code KEEP attributes}). Reading
 * the typed sub-field column directly (e.g. {@code KEEP attributes.status_code}) does not go through this
 * loader and still returns the native value.
 * <p>
 * Everything except {@link #write(XContentBuilder)} is delegated unchanged; {@code write} captures the
 * delegate's output and re-emits it with scalar numbers/booleans turned into strings. The capture round-trip
 * is acceptable because this path materializes the entire flattened object per document anyway.
 */
final class StringifyingFlattenedSubFieldLoader implements SourceLoader.SyntheticFieldLoader {
    private final SourceLoader.SyntheticFieldLoader delegate;

    StringifyingFlattenedSubFieldLoader(SourceLoader.SyntheticFieldLoader delegate) {
        this.delegate = delegate;
    }

    @Override
    public Stream<Map.Entry<String, StoredFieldLoader>> storedFieldLoaders() {
        return delegate.storedFieldLoaders();
    }

    @Override
    public DocValuesLoader docValuesLoader(LeafReader leafReader, int[] docIdsInLeaf) throws IOException {
        return delegate.docValuesLoader(leafReader, docIdsInLeaf);
    }

    @Override
    public void prepare() {
        delegate.prepare();
    }

    @Override
    public boolean hasValue() {
        return delegate.hasValue();
    }

    @Override
    public boolean setIgnoredValues(Map<String, List<IgnoredSourceFieldMapper.NameValue>> objectsWithIgnoredFields) {
        return delegate.setIgnoredValues(objectsWithIgnoredFields);
    }

    @Override
    public String fieldName() {
        return delegate.fieldName();
    }

    @Override
    public void reset() {
        delegate.reset();
    }

    @Override
    public void write(XContentBuilder b) throws IOException {
        BytesReference rendered;
        try (XContentBuilder scratch = XContentFactory.jsonBuilder()) {
            scratch.startObject();
            delegate.write(scratch);
            scratch.endObject();
            rendered = BytesReference.bytes(scratch);
        }
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, rendered.streamInput())) {
            parser.nextToken();
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                b.field(parser.currentName());
                parser.nextToken();
                copyAsString(parser, b);
            }
        }
    }

    /**
     * Copies the value at the parser's current position into {@code b}, turning every scalar (number,
     * boolean, string) into a JSON string while preserving array, object, and {@code null} structure.
     */
    private static void copyAsString(XContentParser parser, XContentBuilder b) throws IOException {
        switch (parser.currentToken()) {
            case START_ARRAY -> {
                b.startArray();
                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                    copyAsString(parser, b);
                }
                b.endArray();
            }
            case START_OBJECT -> {
                b.startObject();
                while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                    b.field(parser.currentName());
                    parser.nextToken();
                    copyAsString(parser, b);
                }
                b.endObject();
            }
            case VALUE_NULL -> b.nullValue();
            default -> b.value(parser.text());
        }
    }
}
