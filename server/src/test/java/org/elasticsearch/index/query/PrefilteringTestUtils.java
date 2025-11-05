/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class PrefilteringTestUtils {

    public static void setRandomTermQueryPrefilters(PrefilteredQuery<?> queryBuilder, String... termFieldNames) {
        List<QueryBuilder> filters = new ArrayList<>();
        int numFilters = randomIntBetween(1, 5);
        for (int i = 0; i < numFilters; i++) {
            String filterFieldName = randomFrom(termFieldNames);
            filters.add(QueryBuilders.termQuery(filterFieldName, randomAlphaOfLength(10)));
        }
        queryBuilder.setPrefilters(filters);
    }

}
