/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.xpack.esql.action.AbstractEsqlIntegTestCase;
import org.elasticsearch.xpack.esql.action.ColumnInfoImpl;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.hamcrest.Matchers;

import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;

public class BucketTestIT extends AbstractEsqlIntegTestCase {

    public void testReturnColumnBucketMetadata() {
        try (var response = run(syncEsqlQueryRequest("""
            ROW date=TO_DATETIME("1985-07-09T00:00:00.000Z")
            | STATS date=VALUES(date) BY bucket=BUCKET(date, 20, "1985-01-01T00:00:00Z", "1986-01-01T00:00:00Z")
            """))) {
            assertThat(findColumn(response, "bucket").meta(), Matchers.equalTo(Map.of("bucket", Map.of("date_range", "1 month"))));
        }
    }

    private static ColumnInfoImpl findColumn(EsqlQueryResponse response, String name) {
        return response.columns().stream().filter(c -> Objects.equals(c.name(), name)).findFirst().orElseThrow();
    }
}
