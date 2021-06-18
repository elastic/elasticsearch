/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FieldUsageStats implements ToXContentFragment, Writeable {
    public static final String TOTAL = "total";
    public static final String TERMS = "terms";
    public static final String FREQS = "frequencies";
    public static final String POSITIONS = "positions";
    public static final String OFFSETS = "offsets";
    public static final String DOC_VALUES = "doc_values";
    public static final String STORED_FIELDS = "stored_fields";
    public static final String NORMS = "norms";
    public static final String PAYLOADS = "payloads";
    public static final String TERM_VECTORS = "term_vectors"; // possibly refine this one
    public static final String POINTS = "points";
    public static final String PROXIMITY = "proximity";

    private final Map<String, PerFieldUsageStats> stats;

    public FieldUsageStats() {
        this.stats = new HashMap<>();
    }

    public FieldUsageStats(Map<String, PerFieldUsageStats> stats) {
        this.stats = new HashMap<>(stats);
    }

    public FieldUsageStats(StreamInput in) throws IOException {
        stats = in.readMap(StreamInput::readString, PerFieldUsageStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(stats, StreamOutput::writeString, (o, v) -> v.writeTo(o));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("all_fields");
        total().toXContent(builder, params);
        builder.endObject();

        builder.startObject("fields");
        {
            final List<String> sortedFields = stats.keySet().stream().sorted().collect(Collectors.toList());
            for (String field : sortedFields) {
                builder.startObject(field);
                stats.get(field).toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endObject();
        return builder;
    }

    PerFieldUsageStats total() {
        final PerFieldUsageStats total = new PerFieldUsageStats();
        for (PerFieldUsageStats value : stats.values()) {
            total.add(value);
        }
        return total;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    public boolean hasField(String field) {
        return stats.containsKey(field);
    }

    public PerFieldUsageStats get(String field) {
        return stats.get(field);
    }

    public FieldUsageStats add(FieldUsageStats other) {
        other.stats.forEach((k, v) -> stats.computeIfAbsent(k, f -> new PerFieldUsageStats()).add(v));
        return this;
    }

    public enum UsageContext {
        DOC_VALUES,
        STORED_FIELDS,
        TERMS,
        FREQS,
        POSITIONS,
        OFFSETS,
        NORMS,
        PAYLOADS,
        TERM_VECTORS, // possibly refine this one
        POINTS,
    }

    public static class PerFieldUsageStats implements ToXContentFragment, Writeable {

        public long terms;
        public long freqs;
        public long positions;
        public long offsets;
        public long docValues;
        public long storedFields;
        public long norms;
        public long payloads;
        public long termVectors;
        public long points;

        public PerFieldUsageStats() {

        }

        private void add(PerFieldUsageStats other) {
            terms += other.terms;
            freqs += other.freqs;
            positions += other.positions;
            offsets += other.offsets;
            docValues += other.docValues;
            storedFields += other.storedFields;
            norms += other.norms;
            payloads += other.payloads;
            termVectors += other.termVectors;
            points += other.points;
        }

        public PerFieldUsageStats(StreamInput in) throws IOException {
            terms = in.readVLong();
            freqs = in.readVLong();
            positions = in.readVLong();
            offsets = in.readVLong();
            docValues = in.readVLong();
            storedFields = in.readVLong();
            norms = in.readVLong();
            payloads = in.readVLong();
            termVectors = in.readVLong();
            points = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(terms);
            out.writeVLong(freqs);
            out.writeVLong(positions);
            out.writeVLong(offsets);
            out.writeVLong(docValues);
            out.writeVLong(storedFields);
            out.writeVLong(norms);
            out.writeVLong(payloads);
            out.writeVLong(termVectors);
            out.writeVLong(points);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(TOTAL, getTotal());
            builder.field(TERMS, terms);
            builder.field(PROXIMITY, getProximity());
            builder.field(STORED_FIELDS, storedFields);
            builder.field(DOC_VALUES, docValues);
            builder.field(POINTS, points);
            builder.field(NORMS, norms);
            builder.field(TERM_VECTORS, termVectors);
            builder.field(FREQS, freqs);
            builder.field(POSITIONS, positions);
            builder.field(OFFSETS, offsets);
            builder.field(PAYLOADS, payloads);
            return builder;
        }

        public Set<UsageContext> keySet() {
            final EnumSet<UsageContext> set = EnumSet.noneOf(UsageContext.class);
            if (terms > 0L) {
                set.add(UsageContext.TERMS);
            }
            if (freqs > 0L) {
                set.add(UsageContext.FREQS);
            }
            if (positions > 0L) {
                set.add(UsageContext.POSITIONS);
            }
            if (offsets > 0L) {
                set.add(UsageContext.OFFSETS);
            }
            if (docValues > 0L) {
                set.add(UsageContext.DOC_VALUES);
            }
            if (storedFields > 0L) {
                set.add(UsageContext.STORED_FIELDS);
            }
            if (norms > 0L) {
                set.add(UsageContext.NORMS);
            }
            if (payloads > 0L) {
                set.add(UsageContext.PAYLOADS);
            }
            if (termVectors > 0L) {
                set.add(UsageContext.TERM_VECTORS);
            }
            if (points > 0L) {
                set.add(UsageContext.POINTS);
            }
            return set;
        }

        public long getTerms() {
            return terms;
        }

        public long getFreqs() {
            return freqs;
        }

        public long getPositions() {
            return positions;
        }

        public long getOffsets() {
            return offsets;
        }

        public long getDocValues() {
            return docValues;
        }

        public long getStoredFields() {
            return storedFields;
        }

        public long getNorms() {
            return norms;
        }

        public long getPayloads() {
            return payloads;
        }

        public long getTermVectors() {
            return termVectors;
        }

        public long getPoints() {
            return points;
        }

        public long getProximity() {
            return freqs + offsets + positions + payloads;
        }

        public long getTotal() {
            return terms + freqs + positions + offsets + docValues + storedFields + norms + payloads + termVectors + points;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
