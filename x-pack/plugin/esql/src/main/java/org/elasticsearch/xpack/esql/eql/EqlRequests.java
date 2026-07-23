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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.core.type.DataType.DATETIME;

/**
 * Builds an {@link EqlSearchRequest} from the {@code EQL <indexPattern> "<query>"} command's index pattern,
 * query string, resolved schema and {@code WITH { ... }} tuning options. Kept small and pure so it is
 * unit-testable without a client.
 *
 * <p>The schema drives the fields the EQL engine fetches per event: one {@link FieldAndFormat} per mapped,
 * convertible {@link FieldAttribute} (synthetics and unsupported columns carry no wire field), with the
 * {@code epoch_millis} format on date columns so the converter reads a stable epoch value.
 *
 * <p>Supported {@code WITH} options (all optional): {@code size}, {@code fetch_size}, {@code timestamp_field},
 * {@code tiebreaker_field}, {@code event_category_field}, {@code result_position} ({@code head}/{@code tail}).
 */
public final class EqlRequests {

    private EqlRequests() {}

    public static EqlSearchRequest build(String query, String indices, List<Attribute> schema, Map<String, Object> options) {
        if (indices == null || indices.isBlank()) {
            throw new EsqlIllegalArgumentException("EQL command requires a non-empty index pattern");
        }
        EqlSearchRequest request = new EqlSearchRequest();
        request.indices(Arrays.stream(indices.split(",")).map(String::trim).filter(s -> s.isEmpty() == false).toArray(String[]::new));
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
        applyOptional(request, options);
        return request;
    }

    /** One fetch entry per mapped field column; synthetics ({@code ReferenceAttribute}) and unsupported columns are skipped. */
    private static List<FieldAndFormat> fetchFields(List<Attribute> schema) {
        List<FieldAndFormat> fields = new ArrayList<>();
        for (Attribute attribute : schema) {
            // UnsupportedAttribute extends FieldAttribute, so exclude it explicitly — it has no extractable wire value.
            if (attribute instanceof FieldAttribute fa && attribute instanceof UnsupportedAttribute == false) {
                String format = fa.dataType() == DATETIME ? "epoch_millis" : null;
                fields.add(new FieldAndFormat(fa.fieldName().string(), format));
            }
        }
        return fields;
    }

    private static void applyOptional(EqlSearchRequest request, Map<String, Object> options) {
        if (options.get("size") instanceof Number size) {
            request.size(size.intValue());
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
