/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.IOFunction;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockStoredFieldsReader;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.support.AbstractXContentParser;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Block loader for the synthetic {@code _unmapped_fields} column produced by
 * {@code SET unmapped_fields="LOAD_ALL"}.
 *
 * <p>For each document it reads {@code _source} and re-serialises the surviving top-level key/value pairs as a JSON object, which the
 * coordinator later flattens into the {@code _unmapped_fields} column. A scalar key survives when it satisfies the full
 * {@link UnmappedFieldsPattern#matches} and holds a value; an object or array key ships more leniently
 * ({@link UnmappedFieldsPattern#objectSubfieldsCouldMatch}) because it flattens to descendant leaves the coordinator filters per name.
 * Documents where nothing survives get a null.
 *
 * <p>Pruning the value-less parts here rather than on the coordinator keeps them off the wire entirely, and is what lets the
 * coordinator turn every key it receives into an {@code _unmapped_fields} entry that carries a value - see
 * {@link UnmappedFields#prune} and {@code ExpandUnmappedFieldsPostProcessor}.
 *
 * <p>Field-level security needs no handling here: it strips denied fields from the {@code _source} this reads, so they never
 * reach the pattern. {@code EsqlSecurityIT#testFieldLevelSecurityFieldDeniedWithUnmappedFieldsLoadAll} holds that down.
 *
 * <p>Leaves matching {@code mappedNestedSubfield} - paths this shard's mapping declares under a {@code nested} parent -
 * are pruned like value-less parts: mapped nested subfields stay null everywhere (see
 * {@code EsqlCapabilities.Cap#FIX_NESTED_SUBFIELD_EXTRACTION}), and only the shard knows its mapping.
 * <p>TODO: share a cached {@code _source} parse with other field-extraction operators.
 */
final class UnmappedFieldsBlockLoader implements BlockLoader {

    private final UnmappedFieldsPattern pattern;
    private final double sourceReservationFactor;
    private final Predicate<String> mappedNestedSubfield;

    UnmappedFieldsBlockLoader(UnmappedFieldsPattern pattern, double sourceReservationFactor, Predicate<String> mappedNestedSubfield) {
        this.pattern = pattern;
        this.sourceReservationFactor = sourceReservationFactor;
        this.mappedNestedSubfield = mappedNestedSubfield;
    }

    @Override
    public Builder builder(BlockFactory factory, int expectedCount) {
        return factory.bytesRefs(expectedCount);
    }

    @Override
    public IOFunction<CircuitBreaker, ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
        return null;
    }

    @Override
    public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) throws IOException {
        return new UnmappedFields(breaker, pattern, sourceReservationFactor, mappedNestedSubfield);
    }

    @Override
    public StoredFieldsSpec rowStrideStoredFieldSpec() {
        return new StoredFieldsSpec(true, false, Set.of());
    }

    @Override
    public boolean supportsOrdinals() {
        return false;
    }

    @Override
    public SortedSetDocValues ordinals(LeafReaderContext context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String toString() {
        return "UnmappedFieldsBlockLoader";
    }

    private static class UnmappedFields extends BlockStoredFieldsReader {
        private final CircuitBreaker breaker;
        private final UnmappedFieldsPattern pattern;
        private final double sourceReservationFactor;
        private final Predicate<String> mappedNestedSubfield;

        UnmappedFields(
            CircuitBreaker breaker,
            UnmappedFieldsPattern pattern,
            double sourceReservationFactor,
            Predicate<String> mappedNestedSubfield
        ) {
            super(breaker);
            this.breaker = breaker;
            this.pattern = pattern;
            this.sourceReservationFactor = sourceReservationFactor;
            this.mappedNestedSubfield = mappedNestedSubfield;
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            Source source = storedFields.source();
            // Covers the surviving values we buffer in the survivors HashMap and the filtered JSON we build from them. The factor is
            // what PlannerSettings.SOURCE_RESERVATION_FACTOR measured for this parse: materialising _source again as a map can cost
            // several times its own bytes, since a deeply nested map carries a lot of object overhead on top of the raw content.
            // Streaming the parse below lets us skip a non-matching key without ever materialising its value. Which keys match and
            // survive is decided by the UnmappedFieldsPattern; in the worst case the whole _source survives and is re-materialised in
            // the survivors HashMap, which is the case this reservation is sized for.
            // TODO constructing the filtered source document still materialises every surviving value, so a single huge top-level value
            // can outgrow this estimate; build it entry-by-entry with a budget instead. Tracked by #161648.
            // TODO the _source read itself is still charged a flat BlockSourceReader.ESTIMATED_SIZE by
            // BlockStoredFieldsReader, so an unusually large _source is under-accounted. Engine-wide, pre-existing.
            long reservation = (long) (source.internalSourceRef().length() * sourceReservationFactor);
            breaker.addEstimateBytesAndMaybeBreak(reservation, "unmapped fields source");
            try (
                XContentParser parser = XContentHelper.createParserNotCompressed(
                    XContentParserConfiguration.EMPTY,
                    source.internalSourceRef(),
                    source.sourceContentType()
                )
            ) {
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    throw new IllegalStateException("expected _source to be a JSON object but was [" + parser.currentToken() + "]");
                }
                // Collect the survivors into a HashMap: the coordinator re-orders the keys anyway, so we can just use whatever ordering
                // HashMap gives us rather than sticking to the original _source ordering. readValue builds HashMaps too, so nested
                // objects use the same ordering.
                Map<String, Object> survivors = new HashMap<>();
                for (String key = parser.nextFieldName(); key != null; key = parser.nextFieldName()) {
                    XContentParser.Token token = parser.nextToken();
                    // A scalar is its own leaf key and must match the full pattern; an object or array can flatten to dotted
                    // descendant leaves the coordinator filters per name, so it ships leniently (see objectSubfieldsCouldMatch).
                    boolean container = token == XContentParser.Token.START_OBJECT || token == XContentParser.Token.START_ARRAY;
                    boolean matched = container ? pattern.objectSubfieldsCouldMatch(key) : pattern.matches(key);
                    if (matched == false) {
                        // A wanted-nothing key is skipped without ever materialising its value.
                        parser.skipChildren();
                        continue;
                    }
                    // Materialise this top-level value so prune can walk it; non-matching keys above are never built.
                    Object value = AbstractXContentParser.readValue(parser, HashMap::new);
                    // The pattern decides whether the key is wanted; prune decides whether what is under it says anything at all.
                    if (prune(key, value)) {
                        survivors.put(key, value);
                    }
                }
                // An empty object would carry no more information than a null, and the coordinator treats the two the same.
                if (survivors.isEmpty()) {
                    builder.appendNull();
                } else {
                    try (XContentBuilder json = XContentFactory.jsonBuilder()) {
                        json.startObject();
                        for (Map.Entry<String, Object> entry : survivors.entrySet()) {
                            json.field(entry.getKey(), entry.getValue());
                        }
                        json.endObject();
                        ((BytesRefBuilder) builder).appendBytesRef(BytesReference.bytes(json).toBytesRef());
                    }
                }
            } finally {
                breaker.addWithoutBreaking(-reservation);
            }
        }

        /**
         * Strips nully values out of a {@code _source} value, consistently with how mapped fields do not track nulls in arrays, empty
         * objects and the like - {@code null}, {@code []}, {@code {}}, {@code [null]}, {@code [{"foo":null},{"bar":[]}]},
         * {@code {"baz":[null],"inga":{}}}.
         * <p>{@code path} is the dotted name the value flattens to; leaves matching {@link #mappedNestedSubfield} are
         * stripped the same way. Arrays do not extend the path, matching how the coordinator derives the dotted leaf names.
         */
        private boolean prune(String path, Object value) {
            if (value instanceof List<?> values) {
                values.removeIf(element -> prune(path, element) == false);
                return values.isEmpty() == false;
            }
            // A nully object says nothing about the field it sits under, so it must neither keep that field's _unmapped_fields key alive
            // nor show up in its value.
            if (value instanceof Map<?, ?> map) {
                map.entrySet().removeIf(entry -> prune(path + "." + entry.getKey(), entry.getValue()) == false);
                return map.isEmpty() == false;
            }
            return value != null && mappedNestedSubfield.test(path) == false;
        }

        @Override
        public String toString() {
            return "UnmappedFieldsBlockLoader.UnmappedFields";
        }
    }
}
