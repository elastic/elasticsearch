/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.reservedstate.service.FileSettingsService.FileSettingsHealthInfo;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.core.Tuple.tuple;

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
            randomBoolean() ? FileSettingsHealthInfo.INDETERMINATE : mutateFileSettingsHealthInfo(FileSettingsHealthInfo.INDETERMINATE)
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
        switch (randomInt(3)) {
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
        }
        return new HealthInfo(diskHealth, dslHealth, repoHealth, fsHealth);
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

    private static FileSettingsHealthInfo mutateFileSettingsHealthInfo(FileSettingsHealthInfo original) {
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
