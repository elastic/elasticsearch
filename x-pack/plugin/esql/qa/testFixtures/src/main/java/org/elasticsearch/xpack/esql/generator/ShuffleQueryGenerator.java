/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.generator;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/** Generates bounded join/union trees with a common schema so every subtree can feed either operator. */
public final class ShuffleQueryGenerator {
    private static final List<String> KEYS = List.of("emp_no", "languages", "first_name");
    private static final String COLUMNS = "emp_no, languages, first_name, salary";

    private final Random random;
    private final List<String> indices;
    private int nextJoin;

    /** Uses the test's random source so its seed reproduces the complete query. */
    public ShuffleQueryGenerator(Random random, List<String> indices) {
        this.random = random;
        this.indices = List.copyOf(indices);
    }

    /** Consumes exact join and union budgets, including operators nested in build-side subqueries. */
    public String query(int joins, int unions) {
        return subtree(joins, unions) + "\n| SORT " + COLUMNS + "\n| LIMIT 10000";
    }

    private String subtree(int joins, int unions) {
        String query;
        if (unions > 0 && (joins == 0 || random.nextBoolean())) {
            int leftJoins = random.nextInt(joins + 1);
            int leftUnions = random.nextInt(unions);
            query = "FROM (" + subtree(leftJoins, leftUnions) + "), (" + subtree(joins - leftJoins, unions - leftUnions - 1) + ")";
        } else if (joins > 0) {
            int leftJoins = random.nextInt(joins);
            int leftUnions = random.nextInt(unions + 1);
            String left = subtree(leftJoins, leftUnions);
            String right = subtree(joins - leftJoins - 1, unions - leftUnions);
            String key = KEYS.get(random.nextInt(KEYS.size()));
            String payload = "build_value_" + nextJoin++;
            query = left
                + "\n| "
                + (random.nextBoolean() ? "LEFT OUTER" : "INNER")
                + " JOIN ("
                + right
                + "\n| KEEP "
                + key
                + ", salary | RENAME salary AS "
                + payload
                + ") ON "
                + key
                + "\n| EVAL salary = COALESCE(salary, 0) + COALESCE("
                + payload
                + ", 0)";
        } else {
            query = "FROM " + indices.get(random.nextInt(indices.size())) + "\n| EVAL emp_no = TO_LONG(emp_no), salary = TO_LONG(salary)";
        }
        if (random.nextInt(3) == 0) {
            query += "\n| WHERE salary >= " + random.nextInt(4);
        }
        if (random.nextInt(3) == 0) {
            String key = KEYS.get(random.nextInt(KEYS.size()));
            List<String> aggregates = new ArrayList<>();
            aggregates.add("salary = SUM(salary)");
            for (String other : KEYS) {
                if (other.equals(key) == false) {
                    aggregates.add(other + " = MAX(" + other + ")");
                }
            }
            query += "\n| STATS " + String.join(", ", aggregates) + " BY " + key;
        }
        return query + "\n| KEEP " + COLUMNS;
    }
}
