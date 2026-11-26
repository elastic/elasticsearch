/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.getValuesList;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.equalTo;

/**
 * {@code DESCRIBE} reports every column, even more than the default limit of rows.
 */
public class DescribeWideSchemaIT extends AbstractEsqlIntegTestCase {

    public void testDescribeReportsEveryColumn() {
        int fieldsPerIndex = between(600, 900);
        Set<String> expected = new HashSet<>();
        for (String index : List.of("wide-1", "wide-2")) {
            List<String> mapping = new ArrayList<>();
            for (int i = 0; i < fieldsPerIndex; i++) {
                String field = index + "_f" + i;
                mapping.add(field);
                mapping.add("type=keyword");
                expected.add(field);
            }
            assertAcked(client().admin().indices().prepareCreate(index).setMapping(mapping.toArray(String[]::new)));
        }
        for (String query : List.of(
            "FROM wide-* | DESCRIBE",
            "FROM wide-* | DESCRIBE | KEEP name",
            "FROM wide-* | DESCRIBE | WHERE type == \"keyword\" | SORT name | KEEP name"
        )) {
            try (var resp = run(syncEsqlQueryRequest(query))) {
                Set<String> names = new HashSet<>();
                for (List<Object> row : getValuesList(resp)) {
                    names.add((String) row.get(0));
                }
                assertThat(query, names, equalTo(expected));
            }
        }
    }
}
