/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * A completion candidate field: its resolved type, plus at most one of {@link #values} or
 * {@link #range}. Both are {@code null} when data nodes were not visited (or statistics were
 * suppressed, e.g. under DLS).
 */
public record FieldSuggestion(String type, @Nullable List<ValueSuggestion> values, @Nullable RangeSuggestion range)
    implements
        ToXContentObject {

    public static FieldSuggestion ofType(String type) {
        return new FieldSuggestion(type, null, null);
    }

    /**
     * A single sampled value for a field, with its raw {@code doc_count} — not a normalized fraction;
     * divide by the response's {@code sampled_doc_count} for a frequency if one is wanted.
     *
     * <p>Three things to be explicit about when reading {@code doc_count}:
     * <ul>
     *     <li><b>Sampled shards only.</b> It's the document count across the shards that were actually
     *     visited (hot-tier by default; see {@code skip_cold}), not a global cluster-wide count; the
     *     {@code skipped_cold}/{@code shards_skipped} response warnings qualify this further.</li>
     *     <li><b>Excludes deleted docs.</b> Lucene's {@code TermsEnum#docFreq()} does not count documents
     *     that have been deleted but not yet merged away, so the response's {@code sampled_doc_count}
     *     (live docs) excludes them too — this is expected, not a bug.</li>
     *     <li><b>Overall field frequency, not prefix-scoped.</b> {@code doc_count} is the count across
     *     <i>all</i> sampled documents that contain this term, regardless of any prefix the user typed
     *     inside the string literal — it is not "count among documents matching the prefix." A
     *     prefix-scoped count would need a real query against the postings lists (expensive); this path
     *     only narrows which <i>terms</i> are returned (via the prefix automaton), not their counts.</li>
     * </ul>
     */
    public record ValueSuggestion(Object value, long docCount) implements ToXContentObject {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("value", value);
            builder.field("doc_count", docCount);
            builder.endObject();
            return builder;
        }
    }

    /** The min/max range observed for a range-eligible field. */
    public record RangeSuggestion(Object min, Object max) implements ToXContentObject {
        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("min", min);
            builder.field("max", max);
            builder.endObject();
            return builder;
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("type", type);
        if (values != null) {
            builder.startArray("values");
            for (ValueSuggestion value : values) {
                value.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (range != null) {
            builder.field("range");
            range.toXContent(builder, params);
        }
        builder.endObject();
        return builder;
    }
}
