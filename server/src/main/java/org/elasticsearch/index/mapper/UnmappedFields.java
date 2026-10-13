/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.IgnoredSourceFieldMapper.IgnoredSourceFormat;
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * The fields of a shard's documents that its mapping does not declare: whether the shard can hold any, what counts as
 * one and how they are read. An unmapped field is a leaf of {@code _source}: an object is walked down to the values
 * below it and is never a value itself, an array contributes each of its elements under the name of the array, and a
 * name spelled with dots is the same field as the path through the objects it names.
 */
public final class UnmappedFields {

    private final IndexSettings indexSettings;
    private final MappingLookup mappingLookup;
    @Nullable
    private final Predicate<String> existsInSegments;

    /**
     * @param existsInSegments whether a segment of the shard has a Lucene field of a given name, or {@code null} when the
     *                         segments cannot be inspected
     */
    public UnmappedFields(IndexSettings indexSettings, MappingLookup mappingLookup, @Nullable Predicate<String> existsInSegments) {
        this.indexSettings = indexSettings;
        this.mappingLookup = mappingLookup;
        this.existsInSegments = existsInSegments;
    }

    /**
     * Whether a document of this shard may hold, in its {@code _source}, a field that the mapping does not declare. It
     * returns {@code false} when the index mode is strict columnar and unmapped fields are not routed to the flattened
     * {@code _unmapped} field, when {@code _source} is synthetic and no segment has an {@code _ignored_source} field, and
     * when {@code _source} is stored, no mapping the index has or had can keep an unmapped field (see
     * {@link IndexSettings#retainsUnmappedFields()} and {@link RootObjectMapper#canRetainUnmappedFields()}) and no segment
     * has an {@code _ignored} field. When the segments cannot be inspected it returns {@code true} unless the index mode
     * decides. An {@code _ignored_source} field also holds values of mapped fields, so a shard that has one may still
     * hold no unmapped field.
     */
    public boolean mayExist() {
        if (indexSettings.getMode().isStrictColumnar() && indexSettings.isFlattenedUnmappedFieldsEnabled() == false) {
            return false;
        }
        if (existsInSegments == null) {
            return true;
        }
        if (mappingLookup.isSourceSynthetic()) {
            return existsInSegments.test(IgnoredSourceFieldMapper.NAME);
        }
        return indexSettings.retainsUnmappedFields()
            || mappingLookup.getMapping().getRoot().canRetainUnmappedFields()
            || existsInSegments.test(IgnoredFieldMapper.NAME);
    }

    /**
     * Whether {@code name}, which this shard does not map, may be an unmapped field of one of its documents. It decides as
     * {@link #extract} does, so that a field is found by name exactly where it is found in {@code _source}.
     */
    public boolean mayExist(String name) {
        return skipped("", name) == false && mayExist();
    }

    /**
     * Returns the unmapped fields of the document {@code source} as one JSON object without any object below it, or
     * {@code null} if it has none: each field is a key, spelled with dots and written in the order the fields are first
     * found, and its value is the value of the field, or an array of them when it has several. A {@code null} is not a
     * value.
     *
     * The document is read once, an object that cannot hold an unmapped field is skipped without being parsed, and a
     * value is written as it is found. Only the values found in an array are held, until the array ends, as its elements
     * can hold several values of a field. A document that spells a field in two ways, with a dot in a key and as a key of
     * an object, is read again and all of its values are held, as the two are values of one field. An object never has
     * the same key twice: documents with duplicate keys are rejected when they are indexed, and the parser rejects them.
     *
     * A path that resolves to a field of this shard is not an unmapped field, nor are the keys of a field whose type
     * takes an object as its value, such as a range. A path below a field of any other type is one. A {@code nested}
     * object is walked like any array of objects.
     */
    @Nullable
    BytesRef extract(BytesReference source, XContentType sourceType) throws IOException {
        return extract(source, sourceType, Filter.ALL);
    }

    /**
     * Returns the unmapped fields of the document {@code source} that {@code filter} includes, as {@link #extract(BytesReference,
     * XContentType)} writes them, or {@code null} if it has none. An object or an array below which {@code filter} includes
     * nothing is skipped without being parsed.
     */
    @Nullable
    BytesRef extract(BytesReference source, XContentType sourceType, Filter filter) throws IOException {
        BytesRef written = extract(source, sourceType, filter, false);
        return written == SPELLED_IN_TWO_WAYS ? extract(source, sourceType, filter, true) : written;
    }

    @Nullable
    BytesRef extract(BytesReference source, XContentType sourceType, boolean holdAll) throws IOException {
        return extract(source, sourceType, Filter.ALL, holdAll);
    }

    /** What {@link #extract(BytesReference, XContentType, boolean)} returns for a document it has to hold the values of. */
    static final BytesRef SPELLED_IN_TWO_WAYS = new BytesRef();

    /**
     * @param holdAll whether to hold the values of the document until it is read, which a document that spells a field
     *                in two ways needs
     * @return {@link #SPELLED_IN_TWO_WAYS} if {@code holdAll} is {@code false} and the document may spell a field in two ways
     */
    @Nullable
    BytesRef extract(BytesReference source, XContentType sourceType, Filter filter, boolean holdAll) throws IOException {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, sourceType);
            XContentBuilder out = XContentFactory.jsonBuilder()
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalStateException("expected an object but was [" + parser.currentToken() + "]");
            }
            Extraction extraction = new Extraction(parser, out, filter);
            if (holdAll) {
                extraction.held = new LinkedHashMap<>();
            }
            boolean walked = extraction.object("");
            assert walked || holdAll == false;
            if (walked && holdAll) {
                extraction.writeHeld();
            }
            if (extraction.wrote) {
                out.endObject();
            }
            if (walked == false) {
                return SPELLED_IN_TWO_WAYS;
            }
            return extraction.wrote ? BytesReference.bytes(out).toBytesRef() : null;
        }
    }

    /**
     * Whether {@code path} is that of a field whose type takes an object as its value, such as a range, a geo point, a
     * histogram or a flattened field, so that nothing at or below it is an unmapped field.
     */
    private boolean skipped(String path) {
        return mappingLookup.getMapper(path) instanceof FieldMapper field && field.supportsParsingObject();
    }

    /**
     * Whether {@code key}, found in the object at {@code prefix}, is skipped along with everything below it. A key spelled
     * with dots is checked as the path through the objects it names.
     */
    private boolean skipped(String prefix, String key) {
        assert prefix.isEmpty() || prefix.endsWith(".") : prefix;
        for (int dot = key.indexOf('.'); dot > 0; dot = key.indexOf('.', dot + 1)) {
            if (skipped(prefix + key.substring(0, dot))) {
                return true;
            }
        }
        return skipped(prefix + key);
    }

    /**
     * Which of the unmapped fields of a document to read, by path.
     */
    public interface Filter {
        /** Every unmapped field. */
        Filter ALL = new Filter() {
            @Override
            public boolean includes(String path) {
                return true;
            }

            @Override
            public boolean mayIncludeBelow(String path) {
                return true;
            }
        };

        /** Whether the field {@code path} is read. */
        boolean includes(String path);

        /** Whether a field below the object {@code path} may be read. Never {@code false} if one of them is. */
        boolean mayIncludeBelow(String path);
    }

    /**
     * One walk of a document, which writes the values of its unmapped fields.
     */
    private final class Extraction {
        private final XContentParser parser;
        private final XContentBuilder out;
        private final Filter filter;
        /**
         * The value of each field found since values are held, or the list of them once it has more than one, and
         * {@code null} while values are written as they are found.
         */
        @Nullable
        private Map<String, Object> held;
        private boolean wrote;

        Extraction(XContentParser parser, XContentBuilder out, Filter filter) {
            this.parser = parser;
            this.out = out;
            this.filter = filter;
        }

        /**
         * Walks the object the parser is positioned on, whose keys are found at {@code prefix}.
         *
         * @return {@code false} if a field may be spelled in two ways while values are written as they are found
         */
        boolean object(String prefix) throws IOException {
            // Two keys of an object lead to the same field only if one of them, which holds an object or an array, is
            // what precedes a dot of the other. Two paths to one field part at the deepest object they share, so they
            // are always two keys of one object, and this object is the only one that needs to compare its keys.
            Set<String> holdingFields = null;
            Set<String> precedingADot = null;
            while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                String key = parser.currentName();
                XContentParser.Token token = parser.nextToken();
                if (skipped(prefix, key)) {
                    parser.skipChildren();
                    continue;
                }
                if (held == null) {
                    for (int dot = key.indexOf('.'); dot >= 0; dot = key.indexOf('.', dot + 1)) {
                        String preceding = key.substring(0, dot);
                        if (holdingFields != null && holdingFields.contains(preceding)) {
                            return false;
                        }
                        if (precedingADot == null) {
                            precedingADot = new HashSet<>();
                        }
                        precedingADot.add(preceding);
                    }
                    if (token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY) {
                        if (precedingADot != null && precedingADot.contains(key)) {
                            return false;
                        }
                        if (holdingFields == null) {
                            holdingFields = new HashSet<>();
                        }
                        holdingFields.add(key);
                    }
                }
                if (value(prefix + key) == false) {
                    return false;
                }
            }
            return true;
        }

        /**
         * Takes the value the parser is positioned on, found at {@code path}: an object is walked down to the fields below
         * it, each element of an array is a value of {@code path}, and anything else is a value of the field {@code path}.
         *
         * @return what {@link #object} returns for an object below
         */
        private boolean value(String path) throws IOException {
            switch (parser.currentToken()) {
                case START_OBJECT -> {
                    if (filter.mayIncludeBelow(path) == false) {
                        parser.skipChildren();
                        return true;
                    }
                    return object(path + ".");
                }
                case START_ARRAY -> {
                    if (filter.includes(path) == false && filter.mayIncludeBelow(path) == false) {
                        parser.skipChildren();
                        return true;
                    }
                    boolean outermost = held == null;
                    if (outermost) {
                        held = new LinkedHashMap<>();
                    }
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        boolean walked = value(path);
                        assert walked : "values are held in an array";
                    }
                    if (outermost) {
                        writeHeld();
                        held = null;
                    }
                }
                case VALUE_NULL -> {
                }
                case VALUE_STRING, VALUE_NUMBER, VALUE_BOOLEAN, VALUE_EMBEDDED_OBJECT -> {
                    // A name that resolves without a mapper of its own, such as a runtime field, is a field of the shard all the same.
                    if (mappingLookup.getFieldType(path) == null && filter.includes(path)) {
                        if (held == null) {
                            field(path);
                            out.copyCurrentStructure(parser);
                        } else {
                            hold(path);
                        }
                    }
                }
                default -> throw new IllegalStateException("unexpected token [" + parser.currentToken() + "] at [" + path + "]");
            }
            return true;
        }

        private void hold(String path) throws IOException {
            Object value = switch (parser.currentToken()) {
                case VALUE_STRING -> parser.text();
                case VALUE_NUMBER -> parser.numberValue();
                case VALUE_BOOLEAN -> parser.booleanValue();
                case VALUE_EMBEDDED_OBJECT -> parser.binaryValue();
                default -> throw new IllegalStateException("unexpected token [" + parser.currentToken() + "] at [" + path + "]");
            };
            Object existing = held.putIfAbsent(path, value);
            if (existing instanceof Values values) {
                values.add(value);
            } else if (existing != null) {
                Values values = new Values();
                values.add(existing);
                values.add(value);
                held.put(path, values);
            }
        }

        void writeHeld() throws IOException {
            for (Map.Entry<String, Object> field : held.entrySet()) {
                field(field.getKey());
                out.value(field.getValue());
            }
        }

        private void field(String path) throws IOException {
            if (wrote == false) {
                out.startObject();
                wrote = true;
            }
            out.field(path);
        }
    }

    /** The values of a field that has more than one, in the order they were found. */
    private static final class Values extends ArrayList<Object> {}

    /**
     * Appends to {@code out} the keyword values that a value read from {@code _source} for an unmapped field stands for: an
     * object has none, an array has those of each of its elements, and anything else is one value, rendered as text.
     */
    public static void collectKeywords(@Nullable Object value, List<BytesRef> out) {
        if (value == null || value instanceof Map) {
            return;
        }
        if (value instanceof List<?> list) {
            for (Object element : list) {
                collectKeywords(element, out);
            }
            return;
        }
        out.add(new BytesRef(value.toString()));
    }

    /**
     * Loads the unmapped field {@code name} as keywords, looked up in the parsed {@code _source} that the loaders of a
     * document share. Loads nothing where {@link #mayExist(String)} rules it out.
     */
    public BlockLoader blockLoader(String name) {
        if (mayExist(name) == false) {
            return ConstantNull.INSTANCE;
        }
        // Naming the paths restricts the read to them; without any, the whole of _source is loaded.
        Set<String> sourcePaths = mappingLookup.isSourceEnabled() ? mappingLookup.sourcePaths(name) : Set.of();
        return new NamedFieldBlockLoader(name, sourcePaths, indexSettings.getIgnoredSourceFormat());
    }

    /**
     * Loads, for each document, the unmapped fields that {@code filter} includes as one JSON object, as {@link #extract}
     * writes it. The serialized {@code _source} is read as a stream and never parsed as a whole. A document without any
     * loads as {@code null}. Loads nothing where {@link #mayExist()} rules it out.
     *
     * @param sourceReservationFactor how many times the size of a serialized {@code _source} to reserve while it is read
     * @param filter                  which of the unmapped fields to load
     */
    public BlockLoader allFieldsBlockLoader(double sourceReservationFactor, Filter filter) {
        if (mayExist() == false) {
            return ConstantNull.INSTANCE;
        }
        return new AllFieldsBlockLoader(this, sourceReservationFactor, filter);
    }

    private abstract static class SourceBlockLoader implements BlockLoader {
        @Override
        public final Builder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        public final IOFunction<CircuitBreaker, ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
            return null;
        }

        @Override
        public final boolean supportsOrdinals() {
            return false;
        }

        @Override
        public final SortedSetDocValues ordinals(LeafReaderContext context) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class NamedFieldBlockLoader extends SourceBlockLoader {
        private final String name;
        private final Set<String> sourcePaths;
        private final IgnoredSourceFormat ignoredSourceFormat;

        NamedFieldBlockLoader(String name, Set<String> sourcePaths, IgnoredSourceFormat ignoredSourceFormat) {
            this.name = name;
            this.sourcePaths = sourcePaths;
            this.ignoredSourceFormat = ignoredSourceFormat;
        }

        @Override
        public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) {
            return new BlockStoredFieldsReader(breaker) {
                private final List<BytesRef> values = new ArrayList<>();

                @Override
                public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
                    values.clear();
                    collectKeywords(storedFields.source().extractValue(name, null), values);
                    appendKeywords(values, (BytesRefBuilder) builder);
                }

                @Override
                public String toString() {
                    return "UnmappedFields.Named.Reader[" + name + "]";
                }
            };
        }

        @Override
        public StoredFieldsSpec rowStrideStoredFieldSpec() {
            return StoredFieldsSpec.withSourcePaths(ignoredSourceFormat, sourcePaths);
        }

        @Override
        public String toString() {
            return "UnmappedFields.Named[" + name + "]";
        }
    }

    private static void appendKeywords(List<BytesRef> values, BlockLoader.BytesRefBuilder builder) {
        if (values.isEmpty()) {
            builder.appendNull();
        } else if (values.size() == 1) {
            builder.appendBytesRef(values.get(0));
        } else {
            builder.beginPositionEntry();
            for (BytesRef value : values) {
                builder.appendBytesRef(value);
            }
            builder.endPositionEntry();
        }
    }

    private static final class AllFieldsBlockLoader extends SourceBlockLoader {
        private final UnmappedFields unmappedFields;
        private final double sourceReservationFactor;
        private final Filter filter;

        AllFieldsBlockLoader(UnmappedFields unmappedFields, double sourceReservationFactor, Filter filter) {
            this.unmappedFields = unmappedFields;
            this.sourceReservationFactor = sourceReservationFactor;
            this.filter = filter;
        }

        @Override
        public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) {
            return new BlockStoredFieldsReader(breaker) {
                @Override
                public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
                    Source source = storedFields.source();
                    BytesReference sourceBytes = source.internalSourceRef();
                    // Covers the JSON that is built and the values held to build it, which are at most proportional to the serialized size.
                    long reservation = (long) (sourceBytes.length() * sourceReservationFactor);
                    breaker.addEstimateBytesAndMaybeBreak(reservation, "unmapped fields source");
                    BytesRef fields;
                    try {
                        fields = unmappedFields.extract(sourceBytes, source.sourceContentType(), filter);
                    } finally {
                        breaker.addWithoutBreaking(-reservation);
                    }
                    // The builder accounts for what it holds, so the reservation is released before it grows.
                    if (fields == null) {
                        builder.appendNull();
                    } else {
                        ((BytesRefBuilder) builder).appendBytesRef(fields);
                    }
                }

                @Override
                public String toString() {
                    return "UnmappedFields.All.Reader";
                }
            };
        }

        @Override
        public StoredFieldsSpec rowStrideStoredFieldSpec() {
            return new StoredFieldsSpec(true, false, Set.of());
        }

        @Override
        public String toString() {
            return "UnmappedFields.All";
        }
    }
}
