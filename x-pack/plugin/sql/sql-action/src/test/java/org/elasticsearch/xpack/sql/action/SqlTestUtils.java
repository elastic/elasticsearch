/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.action;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;

import java.util.Random;

public final class SqlTestUtils {

    private SqlTestUtils() {

    }

    /**
     * Returns a random QueryBuilder or null
     */
    public static QueryBuilder randomFilterOrNull(Random random) {
        final QueryBuilder randomFilter;
        if (random.nextBoolean()) {
            randomFilter = randomFilter(random);
        } else {
            randomFilter = null;
        }
        return randomFilter;
    }

    /**
     * Returns a random QueryBuilder
     */
    public static QueryBuilder randomFilter(Random random) {
        return new RangeQueryBuilder(RandomStrings.randomAsciiLettersOfLength(random, 10))
                    .gt(random.nextInt());
    }

}
