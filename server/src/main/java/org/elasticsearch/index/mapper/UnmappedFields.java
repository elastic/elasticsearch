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
import org.elasticsearch.index.mapper.blockloader.ConstantNull;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.mapper.flattened.UnmappedSinkFieldMapper;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
     * Whether a document of this shard may hold, in its {@code _source}, a field that the mapping does not declare. When the
     * index mode is strict columnar, only fields absorbed by the flattened {@code _unmapped} sink are read, so it
     * returns {@code false} when the index has no sink or no segment has the sink's keyed field. Otherwise, it returns
     * {@code false} when {@code _source} is synthetic and no segment has an {@code _ignored_source} field, and
     * when {@code _source} is stored, no mapping the index has or had can keep an unmapped field (see
     * {@link IndexSettings#retainsUnmappedFields()} and {@link RootObjectMapper#canRetainUnmappedFields()}) and no segment
     * has an {@code _ignored} field. When the segments cannot be inspected it returns {@code true} unless the index mode
     * decides. An {@code _ignored_source} field also holds values of mapped fields, so a shard that has one may still hold
     * no unmapped field.
     */
    public boolean mayExist() {
        if (indexSettings.getMode().isStrictColumnar()) {
            // A sink with no keyed field in any segment has absorbed nothing, e.g. because the root's dynamic was never flattened.
            return UnmappedSinkFieldMapper.isEnabled(indexSettings)
                && (existsInSegments == null
                    || existsInSegments.test(FlattenedFieldMapper.UNMAPPED_SINK_NAME + FlattenedFieldMapper.KEYED_FIELD_SUFFIX));
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
        return walk(source, sourceType, new Walk(Set.of(), filter, 0)).written();
    }

    @Nullable
    BytesRef extract(BytesReference source, XContentType sourceType, boolean holdAll) throws IOException {
        return extract(source, sourceType, Filter.ALL, holdAll);
    }

    /**
     * @param holdAll whether to hold the values of the document until it is read, which a document that spells a field
     *                in two ways needs
     * @return {@link #SPELLED_IN_TWO_WAYS} if {@code holdAll} is {@code false} and the document may spell a field in two ways
     */
    @Nullable
    BytesRef extract(BytesReference source, XContentType sourceType, Filter filter, boolean holdAll) throws IOException {
        Found found = walk(source, sourceType, new Walk(Set.of(), filter, 0), holdAll);
        return found == null ? SPELLED_IN_TWO_WAYS : found.written();
    }

    /** What {@link #extract(BytesReference, XContentType, Filter, boolean)} returns for a document it has to hold the values of. */
    static final BytesRef SPELLED_IN_TWO_WAYS = new BytesRef();

    /**
     * What one walk of a document reads: the unmapped fields {@code names}, each loaded on its own, and the unmapped
     * fields that {@code filter} includes, loaded as one object, unless it is {@code null}. The loaders made with one
     * {@code Walk} share the walk of a document.
     */
    public static final class Walk {
        private final Set<String> names;
        /** The paths of the objects that the names are below. */
        private final Set<String> aboveNames;
        @Nullable
        private final Filter filter;
        private final double sourceReservationFactor;

        /**
         * @param sourceReservationFactor how many times the size of a serialized {@code _source} to reserve while a document
         *                                is walked for the fields that {@code filter} includes
         */
        public Walk(Set<String> names, @Nullable Filter filter, double sourceReservationFactor) {
            this.names = Set.copyOf(names);
            this.aboveNames = new HashSet<>();
            for (String name : names) {
                for (int dot = name.indexOf('.'); dot >= 0; dot = name.indexOf('.', dot + 1)) {
                    aboveNames.add(name.substring(0, dot));
                }
            }
            this.filter = filter;
            this.sourceReservationFactor = sourceReservationFactor;
        }

        private boolean mayIncludeBelow(String path) {
            return aboveNames.contains(path) || (filter != null && filter.mayIncludeBelow(path));
        }
    }

    /**
     * What a walk found in a document: the unmapped fields its filter includes, as {@link #extract(BytesReference,
     * XContentType)} writes them, and the values of each of its names, as text. The values of a name are in the order a
     * lookup in {@code _source} parsed into a map finds them: where an object has several keys that lead to the name, those
     * of the keys with fewer dots come first, and the values of one key are in the order they are found.
     */
    record Found(@Nullable BytesRef written, Map<String, NamedValues> named) {
        /** The values found of {@code name}, in order. */
        List<BytesRef> valuesOf(String name) {
            NamedValues values = named.get(name);
            return values == null ? List.of() : values.ordered();
        }
    }

    Found walk(BytesReference source, XContentType sourceType, Walk walk) throws IOException {
        Found found = walk(source, sourceType, walk, false);
        return found == null ? walk(source, sourceType, walk, true) : found;
    }

    /**
     * @param holdAll whether to hold the values of the document until it is read, which a document that spells a field
     *                in two ways needs
     * @return {@code null} if {@code holdAll} is {@code false} and the document may spell a field in two ways
     */
    @Nullable
    private Found walk(BytesReference source, XContentType sourceType, Walk walk, boolean holdAll) throws IOException {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, sourceType);
            XContentBuilder out = walk.filter == null ? null : XContentFactory.jsonBuilder()
        ) {
            if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalStateException("expected an object but was [" + parser.currentToken() + "]");
            }
            Extraction extraction = new Extraction(parser, out, walk);
            if (holdAll && out != null) {
                extraction.held = new LinkedHashMap<>();
            }
            boolean walked = extraction.object("");
            assert walked || holdAll == false;
            if (walked && extraction.held != null) {
                extraction.writeHeld();
            }
            if (extraction.wrote) {
                out.endObject();
            }
            if (walked == false) {
                return null;
            }
            return new Found(extraction.wrote ? BytesReference.bytes(out).toBytesRef() : null, extraction.named);
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
     * One walk of a document, which writes the values of the unmapped fields its filter includes and collects those of its
     * names.
     */
    private final class Extraction {
        /** What {@link #reads} returns when the walk reads the field at a path as one of its names. */
        private static final int NAMED = 1;
        /** What {@link #reads} returns when the walk writes the field at a path. */
        private static final int WRITTEN = 2;
        /** What {@link #reads} returns when the walk reads fields below a path. */
        private static final int BELOW = 4;

        private final XContentParser parser;
        /** Where the fields the filter includes are written, or {@code null} if the walk has no filter. */
        @Nullable
        private final XContentBuilder out;
        private final Walk walk;
        /** The values found of each name of the walk. */
        private Map<String, NamedValues> named = Map.of();
        /**
         * How the path to the value the parser is on is spelled, while the walk has names: the number of dots of the key of
         * each object it is in and the position in each array it is in, outermost first, in the first {@code depth} slots.
         */
        private int[] spelling;
        private int depth;
        /**
         * The value of each field found since values are held, or the list of them once it has more than one, and
         * {@code null} while values are written as they are found.
         */
        @Nullable
        private Map<String, Object> held;
        private boolean wrote;

        Extraction(XContentParser parser, @Nullable XContentBuilder out, Walk walk) {
            this.parser = parser;
            this.out = out;
            this.walk = walk;
            this.spelling = walk.names.isEmpty() ? null : new int[8];
        }

        private void enterKey(String key) {
            if (spelling != null) {
                int dots = 0;
                for (int dot = key.indexOf('.'); dot >= 0; dot = key.indexOf('.', dot + 1)) {
                    dots++;
                }
                enter(dots);
            }
        }

        private void enterPosition(int position) {
            if (spelling != null) {
                enter(position);
            }
        }

        private void enter(int step) {
            if (depth == spelling.length) {
                spelling = Arrays.copyOf(spelling, depth * 2);
            }
            spelling[depth++] = step;
        }

        private void leave() {
            if (spelling != null) {
                depth--;
            }
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
                String path = prefix.isEmpty() ? key : prefix + key;
                int reads = reads(token, path);
                if (reads == 0 || skipped(prefix, key)) {
                    parser.skipChildren();
                    continue;
                }
                if (out != null && held == null) {
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
                enterKey(key);
                boolean walked = value(path, reads);
                leave();
                if (walked == false) {
                    return false;
                }
            }
            return true;
        }

        /**
         * What the walk reads of the value at {@code path} that starts with {@code token}: the field {@code path} as one of
         * its names ({@link #NAMED}) or to write it ({@link #WRITTEN}), and fields below it ({@link #BELOW}). The filter is
         * asked once for a key, and its answer holds for each element of an array.
         */
        private int reads(XContentParser.Token token, String path) {
            return switch (token) {
                case START_OBJECT -> walk.mayIncludeBelow(path) ? BELOW : 0;
                case START_ARRAY -> {
                    int field = readsField(path);
                    yield walk.mayIncludeBelow(path) ? field | BELOW : field;
                }
                case VALUE_NULL -> 0;
                default -> readsField(path);
            };
        }

        private int readsField(String path) {
            int reads = walk.names.contains(path) ? NAMED : 0;
            return out != null && walk.filter.includes(path) ? reads | WRITTEN : reads;
        }

        /**
         * Takes the value the parser is positioned on, found at {@code path}, of which the walk reads {@code reads}: an
         * object is walked down to the fields below it, each element of an array is a value of {@code path}, and anything
         * else is a value of the field {@code path}.
         *
         * @return what {@link #object} returns for an object below
         */
        private boolean value(String path, int reads) throws IOException {
            switch (parser.currentToken()) {
                case START_OBJECT -> {
                    if ((reads & BELOW) == 0) {
                        parser.skipChildren();
                        return true;
                    }
                    return object(path + ".");
                }
                case START_ARRAY -> {
                    boolean outermost = out != null && held == null;
                    if (outermost) {
                        held = new LinkedHashMap<>();
                    }
                    for (int position = 0; parser.nextToken() != XContentParser.Token.END_ARRAY; position++) {
                        enterPosition(position);
                        boolean walked = value(path, reads);
                        leave();
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
                    boolean isNamed = (reads & NAMED) != 0;
                    boolean written = (reads & WRITTEN) != 0;
                    // A name that resolves without a mapper of its own, such as a runtime field, is a field of the shard all the same.
                    if ((isNamed || written) && mappingLookup.getFieldType(path) == null) {
                        Object value = isNamed || held != null ? parsedValue(path) : null;
                        if (isNamed) {
                            if (named.isEmpty()) {
                                named = new HashMap<>();
                            }
                            named.computeIfAbsent(path, k -> new NamedValues())
                                .add(new BytesRef(value.toString()), Arrays.copyOf(spelling, depth));
                        }
                        if (written) {
                            if (held == null) {
                                field(path);
                                out.copyCurrentStructure(parser);
                            } else {
                                hold(path, value);
                            }
                        }
                    }
                }
                default -> throw new IllegalStateException("unexpected token [" + parser.currentToken() + "] at [" + path + "]");
            }
            return true;
        }

        /** The value the parser is positioned on, as parsing {@code _source} into a map reads it. */
        private Object parsedValue(String path) throws IOException {
            return switch (parser.currentToken()) {
                case VALUE_STRING -> parser.text();
                case VALUE_NUMBER -> parser.numberValue();
                case VALUE_BOOLEAN -> parser.booleanValue();
                case VALUE_EMBEDDED_OBJECT -> parser.binaryValue();
                default -> throw new IllegalStateException("unexpected token [" + parser.currentToken() + "] at [" + path + "]");
            };
        }

        private void hold(String path, Object value) {
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

    /** The values found of a name, each with the spelling of its path. Most names have one, which is held without a list. */
    static final class NamedValues {
        private BytesRef first;
        private int[] firstSpelling;
        @Nullable
        private List<BytesRef> values;
        @Nullable
        private List<int[]> spellings;
        private boolean inOrder = true;

        void add(BytesRef value, int[] spelling) {
            if (first == null) {
                first = value;
                firstSpelling = spelling;
                return;
            }
            if (values == null) {
                values = new ArrayList<>();
                spellings = new ArrayList<>();
                values.add(first);
                spellings.add(firstSpelling);
            }
            if (Arrays.compare(spelling, spellings.getLast()) < 0) {
                inOrder = false;
            }
            values.add(value);
            spellings.add(spelling);
        }

        /** The values ordered by the spelling of their paths: the order they are found, unless a key with fewer dots comes later. */
        List<BytesRef> ordered() {
            if (values == null) {
                return List.of(first);
            }
            if (inOrder) {
                return values;
            }
            List<Integer> order = new ArrayList<>(values.size());
            for (int i = 0; i < values.size(); i++) {
                order.add(i);
            }
            order.sort((a, b) -> Arrays.compare(spellings.get(a), spellings.get(b)));
            List<BytesRef> ordered = new ArrayList<>(values.size());
            for (int i : order) {
                ordered.add(values.get(i));
            }
            return ordered;
        }

        void appendTo(BlockLoader.BytesRefBuilder builder) {
            if (values == null) {
                builder.appendBytesRef(first);
                return;
            }
            builder.beginPositionEntry();
            for (BytesRef value : ordered()) {
                builder.appendBytesRef(value);
            }
            builder.endPositionEntry();
        }
    }

    /** The values of a field that has more than one, in the order they were found. */
    private static final class Values extends ArrayList<Object> {}

    /**
     * Loads the unmapped field {@code name} as keywords, with a walk of its own. Loads nothing where {@link #mayExist(String)}
     * rules it out.
     */
    public BlockLoader blockLoader(String name) {
        return blockLoader(name, new Walk(Set.of(name), null, 0));
    }

    /**
     * Loads the unmapped field {@code name}, one of the names of {@code walk}, as keywords, read by the walk of each
     * document that the loaders made with {@code walk} share. Loads nothing where {@link #mayExist(String)} rules it out.
     * Where the {@code _unmapped} sink absorbs unmapped fields, a field that is not below a nested object is read from the sink's doc
     * values instead (see {@link UnmappedSinkFieldMapper#blockLoader(String)}).
     */
    public BlockLoader blockLoader(String name, Walk walk) {
        assert walk.names.contains(name) : name + " is not read by the walk";
        if (mayExist(name) == false) {
            return ConstantNull.INSTANCE;
        }

        // A field absorbed below a nested object is in the sink of the nested doc, which is never loaded by ESQL (ESQL doesn't currently
        // support nested docs), so must be read from _source.
        UnmappedSinkFieldMapper sink = mappingLookup.getMapping().getMetadataMapperByClass(UnmappedSinkFieldMapper.class);
        if (sink != null && mappingLookup.nestedLookup().getNestedParent(name) == null) {
            return sink.blockLoader(name);
        }

        // Naming the paths restricts the read to them; without any, the whole of _source is loaded.
        Set<String> sourcePaths = mappingLookup.isSourceEnabled() ? mappingLookup.sourcePaths(name) : Set.of();
        return new NamedFieldBlockLoader(
            this,
            name,
            walk,
            StoredFieldsSpec.withSourcePaths(indexSettings.getIgnoredSourceFormat(), sourcePaths)
        );
    }

    /**
     * Loads, for each document, the unmapped fields that the filter of {@code walk} includes as one JSON object, as
     * {@link #extract} writes it, read by the walk of each document that the loaders made with {@code walk} share. The
     * serialized {@code _source} is read as a stream and never parsed as a whole. A document without any loads as
     * {@code null}. Loads nothing where {@link #mayExist()} rules it out.
     */
    public BlockLoader allFieldsBlockLoader(Walk walk) {
        assert walk.filter != null : "the walk writes no fields";
        if (mayExist() == false) {
            return ConstantNull.INSTANCE;
        }
        return new AllFieldsBlockLoader(this, walk);
    }

    /** What {@code walk} finds in the current document of {@code storedFields}, walked once for all the loaders of {@code walk}. */
    private Found walk(BlockLoader.StoredFields storedFields, Walk walk, CircuitBreaker breaker) throws IOException {
        return storedFields.shared(walk, fields -> {
            Source source = fields.source();
            BytesReference sourceBytes = source.internalSourceRef();
            // Covers the JSON that is built and the values held to build it, which are at most proportional to the serialized size.
            // It is released before a loader appends to its builder, which accounts for what it holds.
            long reservation = walk.filter == null ? 0 : (long) (sourceBytes.length() * walk.sourceReservationFactor);
            breaker.addEstimateBytesAndMaybeBreak(reservation, "unmapped fields source");
            try {
                return walk(sourceBytes, source.sourceContentType(), walk);
            } finally {
                breaker.addWithoutBreaking(-reservation);
            }
        });
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
        private final UnmappedFields unmappedFields;
        private final String name;
        private final Walk walk;
        private final StoredFieldsSpec storedFieldsSpec;

        NamedFieldBlockLoader(UnmappedFields unmappedFields, String name, Walk walk, StoredFieldsSpec storedFieldsSpec) {
            this.unmappedFields = unmappedFields;
            this.name = name;
            this.walk = walk;
            this.storedFieldsSpec = storedFieldsSpec;
        }

        @Override
        public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) {
            return new BlockStoredFieldsReader(breaker) {
                @Override
                public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
                    NamedValues values = unmappedFields.walk(storedFields, walk, breaker).named().get(name);
                    if (values == null) {
                        builder.appendNull();
                    } else {
                        values.appendTo((BytesRefBuilder) builder);
                    }
                }

                @Override
                public String toString() {
                    return "UnmappedFields.Named.Reader[" + name + "]";
                }
            };
        }

        @Override
        public StoredFieldsSpec rowStrideStoredFieldSpec() {
            return storedFieldsSpec;
        }

        @Override
        public String toString() {
            return "UnmappedFields.Named[" + name + "]";
        }
    }

    private static final class AllFieldsBlockLoader extends SourceBlockLoader {
        private final UnmappedFields unmappedFields;
        private final Walk walk;

        AllFieldsBlockLoader(UnmappedFields unmappedFields, Walk walk) {
            this.unmappedFields = unmappedFields;
            this.walk = walk;
        }

        @Override
        public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) {
            return new BlockStoredFieldsReader(breaker) {
                @Override
                public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
                    BytesRef fields = unmappedFields.walk(storedFields, walk, breaker).written();
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
