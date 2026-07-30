/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.UnsupportedAttribute;
import org.elasticsearch.xpack.esql.core.type.PotentiallyUnmappedKeywordEsField;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;

/**
 * Builds an {@link EqlSearchRequest} from the {@code EQL <indexPattern> "<query>"} command's index pattern,
 * query string, resolved schema and {@code WITH { ... }} tuning options. Kept small and pure so it is
 * unit-testable without a client.
 *
 * <p>The schema drives the fields the EQL engine fetches per event: one {@link FieldAndFormat} per mapped,
 * convertible {@link FieldAttribute} (synthetics, metadata and unsupported columns carry no wire field), with the
 * {@code epoch_millis} format on date columns so the converter reads a stable epoch value. Unmapped columns added
 * under {@code SET unmapped_fields} follow their kind: nullified ({@code NULL}) columns carry no fetch, LOAD columns
 * fetch from {@code _source} with {@code include_unmapped}.
 *
 * <p>The request {@code size} follows an explicit precedence: a {@code WITH {"size"}} option wins; otherwise the
 * row {@code LIMIT} folded into the plan ({@code pushedLimit}); otherwise the ES|QL result-truncation cap
 * ({@code defaultSize}). Only the last case can silently truncate, so the caller warns on it (see
 * {@link #usesTruncationCapSize}).
 *
 * <p>Supported {@code WITH} options (all optional): {@code size}, {@code fetch_size}, {@code timestamp_field},
 * {@code tiebreaker_field}, {@code event_category_field}, {@code result_position} ({@code head}/{@code tail}).
 */
public final class EqlRequests {

    private EqlRequests() {}

    public static EqlSearchRequest build(
        String query,
        String indices,
        List<Attribute> schema,
        Map<String, Object> options,
        Integer pushedLimit,
        int defaultSize
    ) {
        if (indices == null || indices.isBlank()) {
            throw new EsqlIllegalArgumentException("EQL command requires a non-empty index pattern");
        }
        EqlSearchRequest request = new EqlSearchRequest();
        request.indices(Arrays.stream(indices.split(",")).map(String::trim).filter(s -> s.isEmpty() == false).toArray(String[]::new));
        // Resolve and execute over the same index set: ES|QL resolved the schema under IndexResolver.DEFAULT_OPTIONS, so
        // pin the same options here (the command surface differs from standalone _eql/search defaults). This is also the
        // prerequisite that makes reusing the resolved field-caps sound.
        request.indicesOptions(IndexResolver.DEFAULT_OPTIONS);
        request.query(query);
        // Fail loud rather than silently truncate: the cluster default for allow_partial_search_results is true,
        // so a shard failure would otherwise return a clean, incomplete table. A security detection command must
        // not present partial results as complete. Pin both to false until ESQL surfaces partial-results warnings.
        request.allowPartialSearchResults(false);
        request.allowPartialSequenceResults(false);
        List<FieldAndFormat> fetchFields = fetchFields(schema);
        if (fetchFields.isEmpty() == false) {
            request.fetchFields(fetchFields);
        }
        // Effective size default; applyOptional overwrites it with a WITH {"size"} value if present.
        request.size(pushedLimit != null ? pushedLimit : defaultSize);
        applyOptional(request, options);
        return request;
    }

    /**
     * Whether the effective request size came from the truncation cap (no {@code WITH {"size"}} and no pushed
     * {@code LIMIT}) — the only case where a full response may be silently incomplete, so the caller warns on it.
     */
    public static boolean usesTruncationCapSize(Map<String, Object> options, Integer pushedLimit) {
        return hasExplicitSize(options) == false && pushedLimit == null;
    }

    /** Whether {@code WITH {"size": N}} was supplied — the single source of truth for the size-override check. */
    private static boolean hasExplicitSize(Map<String, Object> options) {
        return options.get("size") instanceof Number;
    }

    /**
     * One fetch entry per mapped field column. Synthetics ({@code ReferenceAttribute}), metadata columns
     * ({@code MetadataAttribute} — their values come from the response envelope, not the fields API) and unsupported
     * columns are skipped: only real {@code FieldAttribute}s (excluding the {@code UnsupportedAttribute} subtype) fetch.
     * Unmapped columns added under {@code SET unmapped_fields}: a {@code NULL}-typed (nullified) column carries no fetch
     * entry, while a {@code LOAD}-mode column ({@code PotentiallyUnmappedKeywordEsField}) fetches from {@code _source}
     * with {@code include_unmapped}.
     */
    private static List<FieldAndFormat> fetchFields(List<Attribute> schema) {
        List<FieldAndFormat> fields = new ArrayList<>();
        for (Attribute attribute : schema) {
            // UnsupportedAttribute extends FieldAttribute, so exclude it explicitly — it has no extractable wire value.
            if (attribute instanceof FieldAttribute fa && attribute instanceof UnsupportedAttribute == false) {
                // A nullified unmapped column (SET unmapped_fields=nullify) is NULL-typed and produces no value —
                // the converter constant-nulls it, so it carries no fetch entry.
                if (fa.dataType() == NULL) {
                    continue;
                }
                String format = fa.dataType() == DATETIME ? "epoch_millis" : null;
                // A LOAD-mode unmapped column is backed by PotentiallyUnmappedKeywordEsField; fetch it from _source
                // with include_unmapped=true, the same way FROM loads it.
                if (fa.field() instanceof PotentiallyUnmappedKeywordEsField) {
                    fields.add(new FieldAndFormat(fa.fieldName().string(), format, true));
                } else {
                    fields.add(new FieldAndFormat(fa.fieldName().string(), format));
                }
            }
        }
        return fields;
    }

    private static void applyOptional(EqlSearchRequest request, Map<String, Object> options) {
        if (hasExplicitSize(options)) {
            request.size(((Number) options.get("size")).intValue());
        }
        if (options.get("fetch_size") instanceof Number fetchSize) {
            request.fetchSize(fetchSize.intValue());
        }
        if (options.get("timestamp_field") instanceof String timestampField) {
            request.timestampField(timestampField);
        }
        if (options.get("tiebreaker_field") instanceof String tiebreakerField) {
            request.tiebreakerField(tiebreakerField);
        }
        if (options.get("event_category_field") instanceof String eventCategoryField) {
            request.eventCategoryField(eventCategoryField);
        }
        if (options.get("result_position") instanceof String resultPosition) {
            request.resultPosition(resultPosition);
        }
    }
}
