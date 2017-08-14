/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin.sql.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.sql.plugin.sql.action.SqlResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlResponseTests extends AbstractStreamableTestCase<SqlResponse> {

    @Override
    protected SqlResponse createTestInstance() {
        Map<String, String> columns;
        List<Map<String, Object>> rows;
        if (randomBoolean()) {
            columns = Collections.emptyMap();
        } else {
            int size = randomIntBetween(1, 10);
            columns = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                columns.put(randomAlphaOfLength(10), randomAlphaOfLength(10));
            }
        }

        if (randomBoolean()) {
            rows = Collections.emptyList();
        } else {
            int size = randomIntBetween(1, 10);
            rows = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                Map<String, Object> row = new HashMap<>(size);
                for (int j = 0; i < size; i++) {
                    row.put(randomAlphaOfLength(10), randomBoolean() ? randomAlphaOfLength(10) : randomInt());
                }
                rows.add(row);
            }
        }


        return new SqlResponse(randomAlphaOfLength(10), randomNonNegativeLong(), columns, rows);
    }

    @Override
    protected SqlResponse createBlankInstance() {
        return new SqlResponse();
    }

}
