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
 * vocabulary describing why statistics may be missing or partial.
 *
 * <p>A {@code keyword} field with sampled values:
 * {@snippet lang="json" :
 * {
 *   "status": {
 *     "type": "keyword",
 *     "values": [
 *       { "value": "ok", "docFreq": 0.9 }
 *     ]
 *   }
 * }
 * }
 *
 * <p>A numeric field with a sampled range:
 * {@snippet lang="json" :
 * {
 *   "latency": {
 *     "type": "long",
 *     "range": { "min": 0, "max": 500 }
 *   }
 * }
 * }
 *
 * <p>A field whose statistics were not populated (data nodes not visited) — no {@code values}/
 * {@code range} key present at all, not {@code null}:
 * {@snippet lang="json" :
 * {
 *   "message": {
 *     "type": "text"
 *   }
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
        /** Only hot indices were consulted for a wildcard pattern; cold/frozen tiers were skipped. */
        HOT_ONLY;

        public String wireName() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private final Map<String, FieldSuggestion> fields;
    private final List<Warning> warnings;

    public EsqlSuggestionsResponse(Map<String, FieldSuggestion> fields, List<Warning> warnings) {
        this.fields = fields;
        this.warnings = warnings;
    }

    public EsqlSuggestionsResponse(StreamInput in) throws IOException {
        this.fields = in.readMap(i -> new FieldSuggestion(i.readString(), null, null));
        this.warnings = in.readCollectionAsList(i -> i.readEnum(Warning.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // The response is node-local (produced and consumed on the coordinator). Statistics
        // (values/range) are not part of the transport form yet; only the field/type skeleton and
        // warnings are serialized so this remains a valid Writeable for testing and future use.
        out.writeMap(fields, (o, v) -> o.writeString(v.type()));
        out.writeCollection(warnings, StreamOutput::writeEnum);
    }

    public Map<String, FieldSuggestion> fields() {
        return fields;
    }

    public List<Warning> warnings() {
        return warnings;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
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
