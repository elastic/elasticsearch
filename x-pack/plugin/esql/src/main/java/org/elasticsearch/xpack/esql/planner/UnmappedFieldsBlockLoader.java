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
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Block loader for the synthetic {@code _unmapped_fields} column produced by
 * {@code SET unmapped_fields="LOAD_ALL"}.
 *
 * group and not matching any exclude pattern) and hold a value, and re-serialises the surviving key/value
 * <p>For each document it reads {@code _source} and re-serialises the surviving top-level key/value pairs as a JSON object, which the
 * coordinator later flattens into per-leaf columns. A scalar key survives when it satisfies the full UnmappedFieldsPattern#matches
 * and hold a value; an object or array key ships more leniently ({@link UnmappedFieldsPattern#objectSubfieldsCouldMatch})
 * because it flattens to descendant leaves the coordinator filters per name. Documents where nothing survives get a null.
 *
 * <p>Pruning the value-less parts here rather than on the coordinator keeps them off the wire entirely, and is what lets the
 * coordinator turn every key it receives into an output column without producing one that is null in every row - see
 * {@link UnmappedFields#prune} and {@code ExpandUnmappedFieldsPostProcessor}.
 *
 * <p>Field-level security needs no handling here: it strips denied fields from the {@code _source} this reads, so they never
 * reach the pattern. {@code EsqlSecurityIT#testFieldLevelSecurityFieldDeniedWithUnmappedFieldsLoadAll} holds that down.
 * <p>TODO: share a cached {@code _source} parse with other field-extraction operators.
 */
final class UnmappedFieldsBlockLoader implements BlockLoader {

    private final UnmappedFieldsPattern pattern;
    private final double sourceReservationFactor;

    UnmappedFieldsBlockLoader(UnmappedFieldsPattern pattern, double sourceReservationFactor) {
        this.pattern = pattern;
        this.sourceReservationFactor = sourceReservationFactor;
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
        return new UnmappedFields(breaker, pattern, sourceReservationFactor);
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

        UnmappedFields(CircuitBreaker breaker, UnmappedFieldsPattern pattern, double sourceReservationFactor) {
            super(breaker);
            this.breaker = breaker;
            this.pattern = pattern;
            this.sourceReservationFactor = sourceReservationFactor;
        }

        @Override
        public void read(int docId, StoredFields storedFields, Builder builder) throws IOException {
            Source source = storedFields.source();
            // Covers the parsed map and the JSON we build from it, both of which are proportional to _source. The factor is what
            // PlannerSettings.SOURCE_RESERVATION_FACTOR measured for this very parse: a map costs several times the bytes it came from.
            // TODO the _source read itself is still charged a flat BlockSourceReader.ESTIMATED_SIZE by
            // BlockStoredFieldsReader, so an unusually large _source is under-accounted. Engine-wide, pre-existing.
            long reservation = (long) (source.internalSourceRef().length() * sourceReservationFactor);
            breaker.addEstimateBytesAndMaybeBreak(reservation, "unmapped fields source");
            try {
                Map<String, Object> sourceMap = XContentHelper.convertToMap(source.internalSourceRef(), false, source.sourceContentType())
                    .v2();
                try (XContentBuilder json = XContentFactory.jsonBuilder()) {
                    json.startObject();
                    boolean anyMatch = false;
                    for (Map.Entry<String, Object> entry : sourceMap.entrySet()) {
                        // A scalar becomes its own leaf column and must match the full pattern; an object or array can flatten to dotted
                        // descendant leaves the coordinator filters per name, so it ships leniently (see objectSubfieldsCouldMatch).
                        Object value = entry.getValue();
                        boolean matched = value instanceof Map || value instanceof List
                            ? pattern.objectSubfieldsCouldMatch(entry.getKey())
                            : pattern.matches(entry.getKey());
                        // The pattern decides whether the key is wanted; prune decides whether what is under it says anything at all.
                        if (matched && prune(value)) {
                            anyMatch = true;
                            json.field(entry.getKey(), value);
                        }
                    }
                    json.endObject();
                    // An empty object would carry no more information than a null, and the coordinator treats the two the same.
                    if (anyMatch) {
                        ((BytesRefBuilder) builder).appendBytesRef(BytesReference.bytes(json).toBytesRef());
                    } else {
                        builder.appendNull();
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
         */
        private static boolean prune(Object value) {
            if (value instanceof List<?> values) {
                values.removeIf(element -> prune(element) == false);
                return values.isEmpty() == false;
            }
            // Objects are not expanded into columns of their own, but a nully one still says nothing about the field it sits under, so
            // it must neither keep that field's column alive nor show up in what the field renders as.
            if (value instanceof Map<?, ?> map) {
                map.values().removeIf(element -> prune(element) == false);
                return map.isEmpty() == false;
            }
            return value != null;
        }

        @Override
        public String toString() {
            return "UnmappedFieldsBlockLoader.UnmappedFields";
        }
    }
}
