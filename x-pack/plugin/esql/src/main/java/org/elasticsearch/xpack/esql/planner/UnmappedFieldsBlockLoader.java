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
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.MetadataFieldMapper;
import org.elasticsearch.index.mapper.ObjectMapper;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.esql.plan.logical.UnmappedFieldsPattern;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Block loader for the synthetic {@code _unmapped_fields} column produced by
 * {@code SET unmapped_fields="LOAD_ALL"}.
 *
 * <p>For each document it reads {@code _source}, retains only top-level keys
 * that match the {@link UnmappedFieldsPattern} (matching at least one pattern in every include
 * group and not matching any exclude pattern), and re-serialises the surviving key/value
 * pairs as a JSON object. Documents where nothing survives get a null.
 *
 * <p>Field-level security needs no handling here: it strips denied fields from the {@code _source} this reads, so they never
 * reach the pattern. {@code EsqlSecurityIT#testFieldLevelSecurityFieldDeniedWithUnmappedFieldsLoadAll} holds that down.
 * <p>TODO: share a cached {@code _source} parse with other field-extraction operators.
 */
final class UnmappedFieldsBlockLoader implements BlockLoader {

    /**
     * A no-op block loader that skips {@code _source} reads entirely and emits null for every document.
     * Used when it is known at shard open time that no {@code _source} field can survive the pattern — for
     * example when every top-level field in the shard's mapping is already excluded by {@link #isNoop}.
     */
    static final BlockLoader NOOP = new BlockLoader() {
        @Override
        public Builder builder(BlockFactory factory, int expectedCount) {
            return factory.bytesRefs(expectedCount);
        }

        @Override
        public IOFunction<CircuitBreaker, ColumnAtATimeReader> columnAtATimeReader(LeafReaderContext context) {
            return null;
        }

        @Override
        public RowStrideReader rowStrideReader(CircuitBreaker breaker, LeafReaderContext context) {
            return new RowStrideReader() {
                @Override
                public void read(int docId, StoredFields storedFields, Builder builder) {
                    builder.appendNull();
                }

                @Override
                public boolean canReuse(int startingDocID) {
                    return true;
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public StoredFieldsSpec rowStrideStoredFieldSpec() {
            return StoredFieldsSpec.NO_REQUIREMENTS;
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
            return "NoopUnmappedFieldsBlockLoader";
        }
    };

    /**
     * Returns {@code true} when, on the shard described by {@code mappingLookup}, the given {@code pattern} can never match any
     * {@code _source} field — so every document would produce a null block and the {@code _source} reads may be skipped entirely.
     * <p>
     * For top-level scalar fields (no dot in the full path) it applies {@link UnmappedFieldsPattern#matches}; for top-level object
     * and nested fields it applies the looser {@link UnmappedFieldsPattern#objectSubfieldsCouldMatch}. That check is conservative:
     * an object field whose every descendant is excluded will cause the method to return {@code false}, falling back to the full
     * {@code _source} read. That is safe — the optimisation matters most for flat, fully-mapped indices where no object fields appear.
     * <p>
     * The check is skipped (and returns {@code false}) when the root {@code dynamic} setting is {@code false} or {@code flattened},
     * because in those modes {@code _source} may contain fields that are absent from the mapping.
     */
    static boolean isNoop(UnmappedFieldsPattern pattern, MappingLookup mappingLookup) {
        if (pattern.isNone()) {
            return true;
        }
        // With dynamic:false or dynamic:flattened, _source may contain fields not present in the mapping,
        // so we cannot conclude from the mapping alone that nothing will survive the pattern.
        ObjectMapper.Dynamic rootDynamic = ObjectMapper.Dynamic.getRootDynamic(mappingLookup);
        if (rootDynamic == ObjectMapper.Dynamic.FALSE || rootDynamic == ObjectMapper.Dynamic.FLATTENED) {
            return false;
        }
        // Check top-level scalar fields. MetadataFieldMapper instances (_id, _source, etc.) live outside
        // the user _source document, so they must not be considered here. Dotted paths (e.g. "parent.child")
        // are not top-level _source keys — they are covered by the object-mapper pass below.
        for (Mapper mapper : mappingLookup.fieldMappers()) {
            if (mapper instanceof MetadataFieldMapper) {
                continue;
            }
            String fullPath = mapper.fullPath();
            if (fullPath.indexOf('.') < 0 && pattern.matches(fullPath)) {
                return false;
            }
        }
        // Check top-level object and nested fields.
        for (ObjectMapper objectMapper : mappingLookup.objectMappers().values()) {
            String fullPath = objectMapper.fullPath();
            if (fullPath.indexOf('.') < 0 && pattern.objectSubfieldsCouldMatch(fullPath)) {
                return false;
            }
        }
        return true;
    }

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
                        if (pattern.matches(entry.getKey())) {
                            anyMatch = true;
                            json.field(entry.getKey(), entry.getValue());
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

        @Override
        public String toString() {
            return "UnmappedFieldsBlockLoader.UnmappedFields";
        }
    }
}
