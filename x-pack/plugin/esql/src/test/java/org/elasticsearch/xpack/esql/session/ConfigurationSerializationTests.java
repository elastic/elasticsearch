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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockStreamInput;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.time.Instant;
import java.time.ZoneId;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomTables;

public class ConfigurationSerializationTests extends AbstractWireSerializingTestCase<Configuration> {

    @Override
    protected Writeable.Reader<Configuration> instanceReader() {
        return in -> new Configuration(
            new BlockStreamInput(in, new BlockFactory(new NoopCircuitBreaker(CircuitBreaker.REQUEST), BigArrays.NON_RECYCLING_INSTANCE))
        );
    }

    @Override
    protected Configuration createTestInstance() {
        return randomConfiguration();
    }

    @Override
    protected Configuration mutateInstance(Configuration in) {
        ZoneId zoneId = in.zoneId();
        Instant now = in.now();
        Locale locale = in.locale();
        String username = in.username();
        String clusterName = in.clusterName();
        QueryPragmas pragmas = in.pragmas();
        int resultTruncationMaxSize = in.resultTruncationMaxSize(false);
        int resultTruncationDefaultSize = in.resultTruncationDefaultSize(false);
        String query = in.query();
        boolean profile = in.profile();
        Map<String, Map<String, Column>> tables = in.tables();
        switch (between(0, 10)) {
            case 0 -> zoneId = randomValueOtherThan(zoneId, () -> randomZone().normalized());
            case 1 -> now = randomValueOtherThan(now, () -> randomInstantBetween(Instant.EPOCH, Instant.ofEpochMilli(Long.MAX_VALUE)));
            case 2 -> locale = randomValueOtherThan(in.locale(), () -> randomLocale(random()));
            case 3 -> username = randomAlphaOfLength(15);
            case 4 -> clusterName = randomAlphaOfLength(15);
            case 5 -> pragmas = new QueryPragmas(
                Settings.builder().put(QueryPragmas.EXCHANGE_BUFFER_SIZE.getKey(), between(1, 10)).build()
            );
            case 6 -> resultTruncationMaxSize += randomIntBetween(3, 10);
            case 7 -> resultTruncationDefaultSize += randomIntBetween(3, 10);
            case 8 -> query += randomAlphaOfLength(2);
            case 9 -> profile = false == profile;
            case 10 -> {
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
        return new Configuration(
            zoneId,
            now,
            locale,
            username,
            clusterName,
            pragmas,
            resultTruncationMaxSize,
            resultTruncationDefaultSize,
            query,
            profile,
            tables,
            System.nanoTime(),
            randomBoolean(),
            in.resultTruncationMaxSize(true),
            in.resultTruncationDefaultSize(true),
            null,
            Map.of()
        );
    }
}
