/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.lucene.DataPartitioning;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.action.ParseTables;
import org.elasticsearch.xpack.esql.expression.function.AbstractFunctionTestCase;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.ql.type.DataType;

import java.time.ZoneId;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.session.EsqlConfiguration.QUERY_COMPRESS_THRESHOLD_CHARS;

public class EsqlConfigurationSerializationTests extends AbstractWireSerializingTestCase<EsqlConfiguration> {

    @Override
    protected Writeable.Reader<EsqlConfiguration> instanceReader() {
        return in -> new EsqlConfiguration(
            new BlockStreamInput(in, new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE))
        );
    }

    private static QueryPragmas randomQueryPragmas() {
        return new QueryPragmas(
            Settings.builder().put(QueryPragmas.DATA_PARTITIONING.getKey(), randomFrom(DataPartitioning.values())).build()
        );
    }

    public static EsqlConfiguration randomConfiguration() {
        int len = randomIntBetween(1, 300) + (frequently() ? 0 : QUERY_COMPRESS_THRESHOLD_CHARS);
        return randomConfiguration(randomRealisticUnicodeOfLength(len), randomTables());
    }

    public static EsqlConfiguration randomConfiguration(String query, Map<String, Map<String, Column>> tables) {
        var zoneId = randomZone();
        var locale = randomLocale(random());
        var username = randomAlphaOfLengthBetween(1, 10);
        var clusterName = randomAlphaOfLengthBetween(3, 10);
        var truncation = randomNonNegativeInt();
        var defaultTruncation = randomNonNegativeInt();
        boolean profile = randomBoolean();

        return new EsqlConfiguration(
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
                        BlockUtils.appendValue(builder, AbstractFunctionTestCase.randomLiteral(dataType).value(), type);
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

    @Override
    protected EsqlConfiguration createTestInstance() {
        return randomConfiguration();
    }

    @Override
    protected EsqlConfiguration mutateInstance(EsqlConfiguration in) {
        ZoneId zoneId = in.zoneId();
        Locale locale = in.locale();
        String username = in.username();
        String clusterName = in.clusterName();
        QueryPragmas pragmas = in.pragmas();
        int resultTruncationMaxSize = in.resultTruncationMaxSize();
        int resultTruncationDefaultSize = in.resultTruncationDefaultSize();
        String query = in.query();
        boolean profile = in.profile();
        Map<String, Map<String, Column>> tables = in.tables();
        switch (between(0, 9)) {
            case 0 -> zoneId = randomValueOtherThan(zoneId, () -> randomZone().normalized());
            case 1 -> locale = randomValueOtherThan(in.locale(), () -> randomLocale(random()));
            case 2 -> username = randomAlphaOfLength(15);
            case 3 -> clusterName = randomAlphaOfLength(15);
            case 4 -> pragmas = new QueryPragmas(
                Settings.builder().put(QueryPragmas.EXCHANGE_BUFFER_SIZE.getKey(), between(1, 10)).build()
            );
            case 5 -> resultTruncationMaxSize += randomIntBetween(3, 10);
            case 6 -> resultTruncationDefaultSize += randomIntBetween(3, 10);
            case 7 -> query += randomAlphaOfLength(2);
            case 8 -> profile = false == profile;
            case 9 -> {
                while (true) {
                    Map<String, Map<String, Column>> newTables = null;
                    try {
                        newTables = randomTables();
                        if (false == tables.equals(newTables)) {
                            tables = newTables;
                            newTables = null;
                            break;
                        }
                    } finally {
                        if (newTables != null) {
                            Releasables.close(
                                Releasables.wrap(
                                    Iterators.flatMap(
                                        newTables.values().iterator(),
                                        columns -> Iterators.map(columns.values().iterator(), Column::values)
                                    )
                                )
                            );
                        }
                    }
                }
            }
        }
        return new EsqlConfiguration(
            zoneId,
            locale,
            username,
            clusterName,
            pragmas,
            resultTruncationMaxSize,
            resultTruncationDefaultSize,
            query,
            profile,
            tables
        );

    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Block.getNamedWriteables());
    }
}
