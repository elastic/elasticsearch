/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.eql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.eql.action.EqlSearchRequest;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;

import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/** Unit tests for mapping the {@code EQL "..." WITH { ... }} options to an {@link EqlSearchRequest}. */
public class EqlRequestsTests extends ESTestCase {

    public void testRequiresIndices() {
        EsqlIllegalArgumentException e = expectThrows(
            EsqlIllegalArgumentException.class,
            () -> EqlRequests.build("process where true", Map.of())
        );
        assertThat(e.getMessage(), containsString("[indices]"));
    }

    public void testSingleIndexAndQuery() {
        EqlSearchRequest request = EqlRequests.build("process where true", Map.of("indices", "logs-*"));
        assertThat(request.indices(), arrayContaining("logs-*"));
        assertThat(request.query(), equalTo("process where true"));
    }

    public void testCommaSeparatedIndicesAreSplitAndTrimmed() {
        EqlSearchRequest request = EqlRequests.build("process where true", Map.of("indices", "logs-a, logs-b ,logs-c"));
        assertThat(request.indices(), arrayContaining("logs-a", "logs-b", "logs-c"));
    }

    public void testOptionalTuning() {
        EqlSearchRequest request = EqlRequests.build(
            "process where true",
            Map.of(
                "indices",
                "logs",
                "size",
                42,
                "fetch_size",
                500,
                "timestamp_field",
                "ts",
                "tiebreaker_field",
                "seq",
                "event_category_field",
                "cat",
                "result_position",
                "head"
            )
        );
        assertThat(request.size(), equalTo(42));
        assertThat(request.fetchSize(), equalTo(500));
        assertThat(request.timestampField(), equalTo("ts"));
        assertThat(request.tiebreakerField(), equalTo("seq"));
        assertThat(request.eventCategoryField(), equalTo("cat"));
        assertThat(request.resultPosition(), equalTo("head"));
    }
}
