/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;

import java.util.Arrays;
import java.util.Map;

/**
 * Builds an {@link EqlSearchRequest} from the {@code EQL "<query>"} command's query string and its
 * {@code WITH { ... }} options. Kept small and pure so it is unit-testable without a client.
 *
 * <p>Supported options (all but {@code indices} optional): {@code indices} (required, comma-separated
 * index pattern), {@code size}, {@code fetch_size}, {@code timestamp_field}, {@code tiebreaker_field},
 * {@code event_category_field}, {@code result_position} ({@code head}/{@code tail}).
 */
public final class EqlRequests {

    private EqlRequests() {}

    public static EqlSearchRequest build(String query, Map<String, Object> options) {
        Object indices = options.get("indices");
        if (indices instanceof String indicesString && indicesString.isBlank() == false) {
            EqlSearchRequest request = new EqlSearchRequest();
            request.indices(
                Arrays.stream(indicesString.split(",")).map(String::trim).filter(s -> s.isEmpty() == false).toArray(String[]::new)
            );
            request.query(query);
            applyOptional(request, options);
            return request;
        }
        throw new EsqlIllegalArgumentException(
            "EQL command requires a non-empty [indices] option, e.g. EQL \"...\" WITH {\"indices\": \"logs-*\"}"
        );
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
