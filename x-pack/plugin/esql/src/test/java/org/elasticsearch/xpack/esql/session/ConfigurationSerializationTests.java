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
import org.elasticsearch.xpack.esql.approximation.ApproximationSettings;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;

import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomConfiguration;
import static org.elasticsearch.xpack.esql.ConfigurationTestUtils.randomTables;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

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
        switch (between(0, 10)) {
            case 0 -> builder.setting(
                QuerySettings.TIME_ZONE,
                randomValueOtherThan(QuerySettings.TIME_ZONE.get(in.resolvedSettings()), () -> randomZone().normalized())
            );
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
        }
        return builder.build();
    }

    private static Configuration configWithTimeZoneAndApproximation() {
        ResolvedSettings settings = ResolvedSettings.EMPTY.withOverride(QuerySettings.TIME_ZONE, ZoneId.of("Europe/Paris"))
            .withOverride(QuerySettings.APPROXIMATION, new ApproximationSettings(10000, null));
        return new ConfigurationBuilder(randomConfiguration()).resolvedSettings(settings).build();
    }

    public void testCurrentNodesCarrySettingsInResolvedBlockOnly() throws IOException {
        // Tier 1 — both peers understand esql_resolved_settings. Settings cross only in the generic resolvedSettings
        // block; the legacy time_zone/approximation slots are not written. Both values must survive intact.
        Configuration roundTripped = copyInstance(configWithTimeZoneAndApproximation(), TransportVersion.current());
        assertThat(QuerySettings.TIME_ZONE.get(roundTripped.resolvedSettings()), equalTo(ZoneId.of("Europe/Paris")));
        assertThat(QuerySettings.APPROXIMATION.get(roundTripped.resolvedSettings()).rows(), equalTo(10000));
    }

    public void testOldNodeSynthesizesResolvedSettingsFromLegacyWire() throws IOException {
        // Tier 2 — peer predates esql_resolved_settings but has the approximation slot. time_zone + approximation
        // travel in their legacy positional slots and are synthesized back into a ResolvedSettings on read.
        TransportVersion preResolvedSettings = TransportVersionUtils.getPreviousVersion(
            TransportVersion.fromName("esql_resolved_settings")
        );
        Configuration roundTripped = copyInstance(configWithTimeZoneAndApproximation(), preResolvedSettings);
        assertThat(QuerySettings.TIME_ZONE.get(roundTripped.resolvedSettings()), equalTo(ZoneId.of("Europe/Paris")));
        assertThat(QuerySettings.APPROXIMATION.get(roundTripped.resolvedSettings()).rows(), equalTo(10000));
    }

    public void testVeryOldNodeWithoutApproximationSlot() throws IOException {
        // Tier 3 — peer predates even the approximation slot (esql_query_approximation). time_zone still travels in
        // its legacy field-1 slot, but approximation has nowhere to go on the wire and is dropped (back to default).
        TransportVersion preApproximation = TransportVersionUtils.getPreviousVersion(TransportVersion.fromName("esql_query_approximation"));
        Configuration roundTripped = copyInstance(configWithTimeZoneAndApproximation(), preApproximation);
        assertThat(QuerySettings.TIME_ZONE.get(roundTripped.resolvedSettings()), equalTo(ZoneId.of("Europe/Paris")));
        assertThat(QuerySettings.APPROXIMATION.get(roundTripped.resolvedSettings()), is(nullValue()));
    }
}
