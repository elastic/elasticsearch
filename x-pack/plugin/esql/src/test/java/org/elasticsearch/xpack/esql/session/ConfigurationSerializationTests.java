/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.TransportVersion;
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
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.Column;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomTables;
import static org.hamcrest.Matchers.equalTo;

public class ConfigurationSerializationTests extends AbstractWireSerializingTestCase<Configuration> {

    @Override
    protected Writeable.Reader<Configuration> instanceReader() {
        return in -> new Configuration(
            new BlockStreamInput(
                in,
                BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker(CircuitBreaker.REQUEST)).build()
            )
        );
    }

    @Override
    protected Configuration createTestInstance() {
        return randomConfiguration();
    }

    @Override
    protected Configuration mutateInstance(Configuration in) {
        ConfigurationBuilder builder = new ConfigurationBuilder(in);
        switch (between(0, 11)) {
            case 0 -> builder.zoneId(randomValueOtherThan(in.zoneId(), () -> randomZone().normalized()));
            case 1 -> builder.now(
                randomValueOtherThan(in.now(), () -> randomInstantBetween(Instant.EPOCH, Instant.ofEpochMilli(Long.MAX_VALUE)))
            );
            case 2 -> builder.locale(randomValueOtherThan(in.locale(), () -> randomLocale(random())));
            case 3 -> builder.username(randomAlphaOfLength(15));
            case 4 -> builder.clusterName(randomAlphaOfLength(15));
            case 5 -> builder.pragmas(
                new QueryPragmas(Settings.builder().put(QueryPragmas.EXCHANGE_BUFFER_SIZE.getKey(), between(1, 10)).build())
            );
            case 6 -> builder.resultTruncationMaxSizeRegular(in.resultTruncationMaxSize(false) + randomIntBetween(3, 10));
            case 7 -> builder.resultTruncationDefaultSizeRegular(in.resultTruncationDefaultSize(false) + randomIntBetween(3, 10));
            case 8 -> builder.query(in.query() + randomAlphaOfLength(2));
            case 9 -> builder.profile(in.profile() == false);
            case 10 -> {
                while (true) {
                    Map<String, Map<String, Column>> newTables = null;
                    try {
                        newTables = randomTables();
                        if (false == in.tables().equals(newTables)) {
                            builder.tables(newTables);
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
            case 11 -> builder.grokMatcherWatchdogMs(randomValueOtherThan(in.grokMatcherWatchdogMs(), () -> randomLongBetween(0, 5000)));
        }
        return builder.build();
    }

    public void testOldNodeFallsBackToDefaultWatchdogMs() throws IOException {
        Configuration original = new ConfigurationBuilder(randomConfiguration()).grokMatcherWatchdogMs(500).build();
        TransportVersion preWatchdog = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_grok_watchdog"));
        Configuration roundTripped = copyInstance(original, preWatchdog);
        assertThat(roundTripped.grokMatcherWatchdogMs(), equalTo(1000L));
    }
}
