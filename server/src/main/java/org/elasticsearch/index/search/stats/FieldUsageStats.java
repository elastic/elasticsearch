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
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class FieldUsageStats implements ToXContentObject, Writeable {
    public static final String ANY = "any";
    public static final String INVERTED_INDEX = "inverted_index";
    public static final String TERMS = "terms";
    public static final String POSTINGS = "postings";
    public static final String TERM_FREQUENCIES = "term_frequencies";
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
        builder.startObject();
        builder.startObject("all_fields");
        total().toXContent(builder, params);
        builder.endObject();

        builder.startObject("fields");
        {
            final List<Map.Entry<String, PerFieldUsageStats>> sortedEntries =
                stats.entrySet().stream().sorted(Map.Entry.comparingByKey()).collect(Collectors.toList());
            for (Map.Entry<String, PerFieldUsageStats> entry : sortedEntries) {
                builder.startObject(entry.getKey());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    PerFieldUsageStats total() {
        PerFieldUsageStats total = PerFieldUsageStats.EMPTY;
        for (PerFieldUsageStats value : stats.values()) {
            total = total.add(value);
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
        FieldUsageStats newStats = new FieldUsageStats(stats);
        other.stats.forEach((k, v) -> newStats.stats.merge(k, v, PerFieldUsageStats::add));
        return newStats;
    }

    public enum UsageContext {
        DOC_VALUES,
        STORED_FIELDS,
        TERMS,
        POSTINGS,
        FREQS,
        POSITIONS,
        OFFSETS,
        NORMS,
        PAYLOADS,
        TERM_VECTORS, // possibly refine this one
        POINTS,
    }

    public static class PerFieldUsageStats implements ToXContentFragment, Writeable {

        static final PerFieldUsageStats EMPTY = new PerFieldUsageStats(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

        private final long any;
        private final long proximity;
        private final long terms;
        private final long postings;
        private final long termFrequencies;
        private final long positions;
        private final long offsets;
        private final long docValues;
        private final long storedFields;
        private final long norms;
        private final long payloads;
        private final long termVectors;
        private final long points;

        public PerFieldUsageStats(long any, long proximity, long terms, long postings, long termFrequencies, long positions, long offsets,
                                  long docValues, long storedFields, long norms, long payloads, long termVectors, long points) {
            this.any = any;
            this.proximity = proximity;
            this.terms = terms;
            this.postings = postings;
            this.termFrequencies = termFrequencies;
            this.positions = positions;
            this.offsets = offsets;
            this.docValues = docValues;
            this.storedFields = storedFields;
            this.norms = norms;
            this.payloads = payloads;
            this.termVectors = termVectors;
            this.points = points;
        }

        private PerFieldUsageStats add(PerFieldUsageStats other) {
            return new PerFieldUsageStats(
                any + other.any,
                proximity + other.proximity,
                terms + other.terms,
                postings + other.postings,
                termFrequencies + other.termFrequencies,
                positions + other.positions,
                offsets + other.offsets,
                docValues + other.docValues,
                storedFields + other.storedFields,
                norms + other.norms,
                payloads + other.payloads,
                termVectors + other.termVectors,
                points + other.points);
        }

        public PerFieldUsageStats(StreamInput in) throws IOException {
            any = in.readVLong();
            proximity = in.readVLong();
            terms = in.readVLong();
            postings = in.readVLong();
            termFrequencies = in.readVLong();
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
            out.writeVLong(any);
            out.writeVLong(proximity);
            out.writeVLong(terms);
            out.writeVLong(postings);
            out.writeVLong(termFrequencies);
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
            builder.field(ANY, any);
            builder.startObject(INVERTED_INDEX);
            builder.field(TERMS, terms);
            builder.field(POSTINGS, postings);
            builder.field(TERM_FREQUENCIES, termFrequencies);
            builder.field(POSITIONS, positions);
            builder.field(OFFSETS, offsets);
            builder.field(PAYLOADS, payloads);
            builder.field(PROXIMITY, proximity);
            builder.endObject();
            builder.field(STORED_FIELDS, storedFields);
            builder.field(DOC_VALUES, docValues);
            builder.field(POINTS, points);
            builder.field(NORMS, norms);
            builder.field(TERM_VECTORS, termVectors);
            return builder;
        }

        public Set<UsageContext> keySet() {
            final EnumSet<UsageContext> set = EnumSet.noneOf(UsageContext.class);
            if (terms > 0L) {
                set.add(UsageContext.TERMS);
            }
            if (postings > 0L) {
                set.add(UsageContext.POSTINGS);
            }
            if (termFrequencies > 0L) {
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

        public long getPostings() {
            return postings;
        }

        public long getTermFrequencies() {
            return termFrequencies;
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
            return proximity;
        }

        public long getAny() {
            return any;
        }

        @Override
        public String toString() {
            return Strings.toString(this);
        }
    }
}
