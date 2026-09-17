/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.core.Tuple.tuple;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class HealthInfoTests extends AbstractWireSerializingTestCase<HealthInfo> {
    @Override
    protected Writeable.Reader<HealthInfo> instanceReader() {
        return HealthInfo::new;
    }

    @Override
    protected HealthInfo createTestInstance() {
        var diskInfoByNode = randomMap(0, 10, () -> tuple(randomAlphaOfLength(10), randomDiskHealthInfo()));
        var repositoriesInfoByNode = randomMap(0, 10, () -> tuple(randomAlphaOfLength(10), randomRepoHealthInfo()));
        return new HealthInfo(
            diskInfoByNode,
            randomBoolean() ? randomDslHealthInfo() : null,
            repositoriesInfoByNode,
            randomBoolean() ? FileSettingsHealthInfo.INDETERMINATE : mutateFileSettingsHealthInfo(FileSettingsHealthInfo.INDETERMINATE),
            randomBoolean() ? randomDlmFrozenTransitionsHealthInfo() : null
        );
    }

    @Override
    public HealthInfo mutateInstance(HealthInfo originalHealthInfo) {
        return mutateHealthInfo(originalHealthInfo);
    }

    public static HealthInfo mutateHealthInfo(HealthInfo originalHealthInfo) {
        var diskHealth = originalHealthInfo.diskInfoByNode();
        var dslHealth = originalHealthInfo.dslHealthInfo();
        var repoHealth = originalHealthInfo.repositoriesInfoByNode();
        var fsHealth = originalHealthInfo.fileSettingsHealthInfo();
        var dlmFrozenTransitionsHealth = originalHealthInfo.dlmFrozenTransitionsHealthInfo();
        switch (randomInt(4)) {
            case 0 -> diskHealth = mutateMap(
                originalHealthInfo.diskInfoByNode(),
                () -> randomAlphaOfLength(10),
                HealthInfoTests::randomDiskHealthInfo
            );
            case 1 -> dslHealth = randomValueOtherThan(originalHealthInfo.dslHealthInfo(), HealthInfoTests::randomDslHealthInfo);
            case 2 -> repoHealth = mutateMap(
                originalHealthInfo.repositoriesInfoByNode(),
                () -> randomAlphaOfLength(10),
                HealthInfoTests::randomRepoHealthInfo
            );
            case 3 -> fsHealth = mutateFileSettingsHealthInfo(fsHealth);
            case 4 -> dlmFrozenTransitionsHealth = randomValueOtherThan(
                dlmFrozenTransitionsHealth,
                HealthInfoTests::randomDlmFrozenTransitionsHealthInfo
            );
            default -> throw new IllegalStateException("unexpected random value");
        }
        return new HealthInfo(diskHealth, dslHealth, repoHealth, fsHealth, dlmFrozenTransitionsHealth);
    }

    public void testOlderTransportVersionOmitsDlmFrozenTransitionsHealthInfo() throws IOException {
        FileSettingsHealthInfo distinctFileSettingsInfo = mutateFileSettingsHealthInfo(FileSettingsHealthInfo.INDETERMINATE);
        HealthInfo original = new HealthInfo(Map.of(), null, Map.of(), distinctFileSettingsInfo, randomDlmFrozenTransitionsHealthInfo());
        // Use a version that supports file_settings_health_info but not dlm_frozen_transitions_health_info,
        // verifying only the DLM frozen field is dropped and the file settings field is still round-tripped.
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(
            TransportVersion.fromName("dlm_frozen_transitions_health_info")
        );
        HealthInfo copy = copyInstance(original, oldVersion);
        assertThat(copy.dlmFrozenTransitionsHealthInfo(), nullValue());
        assertThat(copy.fileSettingsHealthInfo(), equalTo(distinctFileSettingsInfo));
    }

    /**
     * Verifies that when a {@link DlmFrozenTransitionsHealthInfo} is round-tripped through a transport version that predates
     * {@code dlm_frozen_transitions_health_state_counts}, the counts are derived from the sample rather than read off the wire.
     */
    public void testOlderTransportVersionDerivesDlmFrozenCountsFromSample() throws IOException {
        // Build a health info with a known sample so we can predict the derived counts.
        ProjectId projectId = randomProjectIdOrDefault();
        Map<ProjectId, Map<String, DlmFrozenTransitionsHealthInfo.TransitionState>> sample = Map.of(
            projectId,
            Map.of(
                "index-a",
                DlmFrozenTransitionsHealthInfo.TransitionState.UNMARKED,
                "index-b",
                DlmFrozenTransitionsHealthInfo.TransitionState.MARKED
            )
        );
        Map<DlmFrozenTransitionsHealthInfo.TransitionState, Integer> counts = new EnumMap<>(
            DlmFrozenTransitionsHealthInfo.TransitionState.class
        );
        counts.put(DlmFrozenTransitionsHealthInfo.TransitionState.UNMARKED, 1);
        counts.put(DlmFrozenTransitionsHealthInfo.TransitionState.MARKED, 1);
        DlmFrozenTransitionsHealthInfo original = new DlmFrozenTransitionsHealthInfo(
            true,
            true,
            true,
            sample,
            2,
            System.currentTimeMillis(),
            60_000L,
            counts
        );

        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(
            TransportVersion.fromName("dlm_frozen_transitions_health_state_counts")
        );
        DlmFrozenTransitionsHealthInfo copy = copyInstance(
            original,
            writableRegistry(),
            (out, v) -> v.writeTo(out),
            DlmFrozenTransitionsHealthInfo::readFrom,
            oldVersion
        );

        // Counts must equal what can be derived from the capped sample.
        assertThat(copy.overdueIndicesCountByState(), equalTo(counts));
    }

    public static DiskHealthInfo randomDiskHealthInfo() {
        return randomBoolean()
            ? new DiskHealthInfo(randomFrom(HealthStatus.values()))
            : new DiskHealthInfo(randomFrom(HealthStatus.values()), randomFrom(DiskHealthInfo.Cause.values()));
    }

    public static DataStreamLifecycleHealthInfo randomDslHealthInfo() {
        return new DataStreamLifecycleHealthInfo(
            randomList(5, () -> new DslErrorInfo(randomAlphaOfLength(100), System.currentTimeMillis(), randomIntBetween(15, 500))),
            randomIntBetween(6, 1000)
        );
    }

    public static RepositoriesHealthInfo randomRepoHealthInfo() {
        return new RepositoriesHealthInfo(randomList(5, () -> randomAlphaOfLength(10)), randomList(5, () -> randomAlphaOfLength(10)));
    }

    public static DlmFrozenTransitionsHealthInfo randomDlmFrozenTransitionsHealthInfo() {
        return new DlmFrozenTransitionsHealthInfo(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomOverdueIndices(),
            // generated independently of overdueIndices' size so a swapped read/write order would be caught
            // by the wire round-trip test.
            randomIntBetween(0, 200),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            // generated independently of the sample so a swapped read/write order is caught by the wire round-trip test.
            randomCountByState()
        );
    }

    public static Map<ProjectId, Map<String, DlmFrozenTransitionsHealthInfo.TransitionState>> randomOverdueIndices() {
        return randomMap(
            0,
            3,
            () -> tuple(
                randomProjectIdOrDefault(),
                randomMap(0, 5, () -> tuple(randomAlphaOfLength(10), randomFrom(DlmFrozenTransitionsHealthInfo.TransitionState.values())))
            )
        );
    }

    public static Map<DlmFrozenTransitionsHealthInfo.TransitionState, Integer> randomCountByState() {
        return randomMap(
            0,
            DlmFrozenTransitionsHealthInfo.TransitionState.values().length,
            () -> tuple(randomFrom(DlmFrozenTransitionsHealthInfo.TransitionState.values()), randomIntBetween(0, 200))
        );
    }

    static FileSettingsHealthInfo mutateFileSettingsHealthInfo(FileSettingsHealthInfo original) {
        long changeCount = randomValueOtherThan(original.changeCount(), ESTestCase::randomNonNegativeLong);
        long failureStreak = randomLongBetween(0, changeCount);
        String mostRecentFailure;
        if (failureStreak == 0) {
            mostRecentFailure = null;
        } else {
            mostRecentFailure = "Random failure #" + randomIntBetween(1000, 9999);
        }
        return new FileSettingsHealthInfo(true, changeCount, failureStreak, mostRecentFailure);
    }

    /**
     * Mutates a {@link Map} by either adding, updating, or removing an entry.
     */
    public static <K, V> Map<K, V> mutateMap(Map<K, V> original, Supplier<K> randomKeySupplier, Supplier<V> randomValueSupplier) {
        Map<K, V> mapCopy = new HashMap<>(original);
        if (original.isEmpty()) {
            mapCopy.put(randomKeySupplier.get(), randomValueSupplier.get());
        } else {
            switch (randomIntBetween(1, 3)) {
                case 1 -> mapCopy.put(randomKeySupplier.get(), randomValueSupplier.get());
                case 2 -> {
                    K someKey = randomFrom(original.keySet());
                    mapCopy.put(someKey, randomValueOtherThan(original.get(someKey), randomValueSupplier));
                }
                case 3 -> {
                    mapCopy.remove(randomFrom(mapCopy.keySet()));
                }
                default -> throw new IllegalStateException();
            }
        }
        return mapCopy;
    }
}
