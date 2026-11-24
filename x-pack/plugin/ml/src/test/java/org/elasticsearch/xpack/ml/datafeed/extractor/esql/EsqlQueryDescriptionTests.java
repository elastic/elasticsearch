/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlClusterInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EsqlQueryDescriptionTests extends ESTestCase {

    public void testRowsShouldBeReadByColumnNameInOutputOrder() {
        // columns deliberately not in the DESCRIBE order
        EsqlQueryDescription description = EsqlQueryDescription.from(
            response(
                List.of(),
                List.of("origin", "name", "interval", "type"),
                List.of(Arrays.asList("computed", "ts", "1 hour", "date"), Arrays.asList("mapped", "host", null, "keyword"))
            )
        );

        assertThat(description.columnTypes(), equalTo(Map.of("ts", "date", "host", "keyword")));
        assertThat(List.copyOf(description.columnTypes().keySet()), equalTo(List.of("ts", "host")));
    }

    public void testExtraColumnsShouldBeIgnored() {
        EsqlQueryDescription description = EsqlQueryDescription.from(
            response(
                List.of(),
                List.of("name", "type", "metric_type", "unit", "description", "future"),
                List.of(Arrays.asList("host", "keyword", "gauge", null, "d", 1))
            )
        );

        assertThat(description.columnTypes(), equalTo(Map.of("host", "keyword")));
    }

    public void testClustersShouldPassThrough() {
        List<EsqlClusterInfo> clusters = List.of(
            new EsqlClusterInfo("", "logs-*", EsqlClusterInfo.Status.SUCCESSFUL),
            new EsqlClusterInfo("east", "logs-*", EsqlClusterInfo.Status.SKIPPED)
        );

        assertThat(EsqlQueryDescription.from(response(clusters, List.of("name", "type"), List.of())).clusters(), equalTo(clusters));
    }

    public void testMissingRequiredColumnShouldFail() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> EsqlQueryDescription.from(response(List.of(), List.of("name"), List.of(List.of("a"))))
        );
        assertThat(e.getMessage(), containsString("type"));
    }

    @SuppressWarnings("unchecked")
    private static EsqlResponse response(List<EsqlClusterInfo> clusters, List<String> header, List<List<Object>> rows) {
        EsqlResponse response = mock(EsqlResponse.class);
        List<ColumnInfo> columns = header.stream().map(name -> {
            ColumnInfo column = mock(ColumnInfo.class);
            when(column.name()).thenReturn(name);
            return column;
        }).toList();
        doReturn(columns).when(response).columns();
        when(response.rows()).thenReturn((Iterable<Iterable<Object>>) (Iterable<?>) rows);
        doReturn(clusters).when(response).clusters();
        return response;
    }
}
