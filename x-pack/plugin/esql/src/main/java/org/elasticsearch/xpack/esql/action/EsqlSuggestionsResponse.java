/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Response for {@code POST /_esql/suggestions}.
 *
 * <p>Always carries a {@code fields} map keyed by field name. A field's {@code values} or
 * {@code range} statistics are present only when data nodes were actually visited for that field;
 * otherwise the field carries just its resolved {@code type}. {@code warnings} is a closed
 * vocabulary describing why statistics may be missing or partial. {@code took} (milliseconds)
 * covers parse, analyze, and any hot-tier sampling. {@code sampled_doc_count} is the total live
 * document count across the hot-tier shards actually visited for value sampling — present only
 * when that path ran (see {@link FieldSuggestion.ValueSuggestion}'s javadoc for what it means and
 * doesn't mean).
 *
 * <p>A {@code keyword} field with sampled values:
 * {@snippet lang="json" :
 * {
 *   "took": 12,
 *   "sampled_doc_count": 1998320,
 *   "fields": {
 *     "status": {
 *       "type": "keyword",
 *       "values": [
 *         { "value": "ok", "doc_count": 1199476 }
 *       ]
 *     }
 *   },
 *   "warnings": ["skipped_cold"]
 * }
 * }
 *
 * <p>A numeric field with a sampled range:
 * {@snippet lang="json" :
 * {
 *   "took": 3,
 *   "fields": {
 *     "latency": {
 *       "type": "long",
 *       "range": { "min": 0, "max": 500 }
 *     }
 *   },
 *   "warnings": []
 * }
 * }
 *
 * <p>A field whose statistics were not populated (data nodes not visited) — no {@code values}/
 * {@code range} key present at all, not {@code null}:
 * {@snippet lang="json" :
 * {
 *   "took": 1,
 *   "fields": {
 *     "message": {
 *       "type": "text"
 *     }
 *   },
 *   "warnings": []
 * }
 * }
 */
public class EsqlSuggestionsResponse extends ActionResponse implements ToXContentObject {

    /**
     * Closed vocabulary of things that can go wrong (or be intentionally limited) while producing
     * suggestions. The wire form is the lowercase enum name.
     */
    public enum Warning {
        /** Some shards were skipped (e.g. pruned by the query filter), so statistics may be partial. */
        SHARDS_SKIPPED,
        /** Sampling used an approximation that can surface values that do not actually match. */
        FALSE_POSITIVES_POSSIBLE,
        /** Document-level security is active, so per-value/range statistics are suppressed. */
        DLS_ACTIVE,
        /**
         * Cold-tier indices were present in the resolved set and were skipped (the default,
         * {@code skip_cold=true} behavior) — only attached when a cold index was actually present and
         * skipped, not unconditionally.
         */
        SKIPPED_COLD,
        /** The request's timeout budget ran out before sampling finished (or before it could start at all). */
        TIMED_OUT;

        public String wireName() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final Map<String, FieldSuggestion> fields;
    private final List<Warning> warnings;
    private final long tookMillis;
    @Nullable
    private final Long sampledDocCount;

    public EsqlSuggestionsResponse(Map<String, FieldSuggestion> fields, List<Warning> warnings) {
        this(fields, warnings, 0L, null);
    }

    public EsqlSuggestionsResponse(
        Map<String, FieldSuggestion> fields,
        List<Warning> warnings,
        long tookMillis,
        @Nullable Long sampledDocCount
    ) {
        this.fields = fields;
        this.warnings = warnings;
        this.tookMillis = tookMillis;
        this.sampledDocCount = sampledDocCount;
    }

    /** A copy of this response with {@code took} set, e.g. once the coordinator knows the final elapsed time. */
    public EsqlSuggestionsResponse withTook(long tookMillis) {
        return new EsqlSuggestionsResponse(fields, warnings, tookMillis, sampledDocCount);
    }

    public EsqlSuggestionsResponse(StreamInput in) throws IOException {
        this.fields = in.readMap(i -> new FieldSuggestion(i.readString(), null, null));
        this.warnings = in.readCollectionAsList(i -> i.readEnum(Warning.class));
        this.tookMillis = in.readVLong();
        this.sampledDocCount = in.readOptionalVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // The response is node-local (produced and consumed on the coordinator). Statistics
        // (values/range) are not part of the transport form yet; only the field/type skeleton and
        // warnings are serialized so this remains a valid Writeable for testing and future use.
        out.writeMap(fields, (o, v) -> o.writeString(v.type()));
        out.writeCollection(warnings, StreamOutput::writeEnum);
        out.writeVLong(tookMillis);
        out.writeOptionalVLong(sampledDocCount);
    }

    public Map<String, FieldSuggestion> fields() {
        return fields;
    }

    public List<Warning> warnings() {
        return warnings;
    }

    public long tookMillis() {
        return tookMillis;
    }

    @Nullable
    public Long sampledDocCount() {
        return sampledDocCount;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("took", tookMillis);
        if (sampledDocCount != null) {
            builder.field("sampled_doc_count", sampledDocCount);
        }
        builder.startObject("fields");
        for (Map.Entry<String, FieldSuggestion> entry : fields.entrySet()) {
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }
        builder.endObject();
        builder.startArray("warnings");
        for (Warning warning : warnings) {
            builder.value(warning.wireName());
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
