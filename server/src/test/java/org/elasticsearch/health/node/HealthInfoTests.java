/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

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
        return new HealthInfo(diskInfoByNode, randomBoolean() ? randomDslHealthInfo() : null, repositoriesInfoByNode);
    }

    @Override
    public HealthInfo mutateInstance(HealthInfo originalHealthInfo) {
        return mutateHealthInfo(originalHealthInfo);
    }

    public static HealthInfo mutateHealthInfo(HealthInfo originalHealthInfo) {
        var randomInt = randomInt(2);
        return new HealthInfo(
            randomInt == 0
                ? mutateMap(originalHealthInfo.diskInfoByNode(), () -> randomAlphaOfLength(10), HealthInfoTests::randomDiskHealthInfo)
                : originalHealthInfo.diskInfoByNode(),
            randomInt == 1
                ? randomValueOtherThan(originalHealthInfo.dslHealthInfo(), HealthInfoTests::randomDslHealthInfo)
                : originalHealthInfo.dslHealthInfo(),
            randomInt == 2
                ? mutateMap(
                    originalHealthInfo.repositoriesInfoByNode(),
                    () -> randomAlphaOfLength(10),
                    HealthInfoTests::randomRepoHealthInfo
                )
                : originalHealthInfo.repositoriesInfoByNode()
        );
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
}
