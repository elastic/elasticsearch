/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.xpack.esql.action.ParseTables;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.HashMap;
import java.util.Map;

import static org.apache.lucene.tests.util.LuceneTestCase.random;
import static org.apache.lucene.tests.util.LuceneTestCase.randomLocale;
import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.frequently;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLengthBetween;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomFrom;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomNonNegativeInt;
import static org.elasticsearch.test.ESTestCase.randomRealisticUnicodeOfLength;
import static org.elasticsearch.test.ESTestCase.randomZone;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.randomLiteral;
import static org.elasticsearch.xpack.esql.session.Configuration.QUERY_COMPRESS_THRESHOLD_CHARS;

/**
 * Test utilities for building random {@link Configuration}s.
 */
public class ConfigurationTestUtils {
    public static Configuration randomConfiguration() {
        int len = randomIntBetween(1, 300) + (frequently() ? 0 : QUERY_COMPRESS_THRESHOLD_CHARS);
        return randomConfiguration(randomRealisticUnicodeOfLength(len), randomTables());
    }

    public static Configuration randomConfiguration(String query, Map<String, Map<String, Column>> tables) {
        var zoneId = randomZone();
        var locale = randomLocale(random());
        var username = randomAlphaOfLengthBetween(1, 10);
        var clusterName = randomAlphaOfLengthBetween(3, 10);
        var truncation = randomNonNegativeInt();
        var defaultTruncation = randomNonNegativeInt();
        boolean profile = randomBoolean();

        return new Configuration(
            zoneId,
            locale,
            username,
            clusterName,
            randomQueryPragmas(),
            truncation,
            defaultTruncation,
            query,
            profile,
            tables
        );
    }

    private static QueryPragmas randomQueryPragmas() {
        return new QueryPragmas(
            Settings.builder().put(QueryPragmas.DATA_PARTITIONING.getKey(), randomFrom(DataPartitioning.values())).build()
        );
    }

    /**
     * Build random "tables" to use for {@link Configuration#tables()}.
     */
    public static Map<String, Map<String, Column>> randomTables() {
        if (randomBoolean()) {
            return Map.of();
        }
        int count = between(1, 10);
        Map<String, Map<String, Column>> tables = new HashMap<>(count);
        try {
            for (int i = 0; i < 10; i++) {
                tables.put(randomAlphaOfLength(i + 1), randomColumns());
            }
            return tables;
        } finally {
            if (tables.size() != count) {
                Releasables.close(
                    Releasables.wrap(
                        Iterators.flatMap(tables.values().iterator(), columns -> Iterators.map(columns.values().iterator(), Column::values))
                    )
                );
            }
        }
    }

    static Map<String, Column> randomColumns() {
        int count = between(1, 10);
        Map<String, Column> columns = new HashMap<>(count);
        int positions = between(1, 10_000);
        try {
            for (int i = 0; i < count; i++) {
                String name = randomAlphaOfLength(i + 1);
                DataType dataType = randomFrom(ParseTables.SUPPORTED_TYPES);
                ElementType type = PlannerUtils.toElementType(dataType);
                try (
                    Block.Builder builder = type.newBlockBuilder(
                        positions,
                        new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE)
                    )
                ) {
                    for (int p = 0; p < positions; p++) {
                        BlockUtils.appendValue(builder, randomLiteral(dataType).value(), type);
                    }
                    columns.put(name, new Column(dataType, builder.build()));
                }
            }
            return columns;
        } finally {
            if (columns.size() != count) {
                Releasables.close(Releasables.wrap(Iterators.map(columns.values().iterator(), Column::values)));
            }
        }
    }
}
