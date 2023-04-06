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

import java.util.HashMap;
import java.util.Map;

public class HealthInfoTests extends AbstractWireSerializingTestCase<HealthInfo> {
    @Override
    protected Writeable.Reader<HealthInfo> instanceReader() {
        return HealthInfo::new;
    }

    @Override
    protected HealthInfo createTestInstance() {
        int numberOfNodes = randomIntBetween(0, 200);
        Map<String, DiskHealthInfo> diskInfoByNode = new HashMap<>(numberOfNodes);
        for (int i = 0; i < numberOfNodes; i++) {
            DiskHealthInfo diskHealthInfo = randomBoolean()
                ? new DiskHealthInfo(randomFrom(HealthStatus.values()))
                : new DiskHealthInfo(randomFrom(HealthStatus.values()), randomFrom(DiskHealthInfo.Cause.values()));
            diskInfoByNode.put(randomAlphaOfLengthBetween(10, 100), diskHealthInfo);
        }
        return new HealthInfo(diskInfoByNode);
    }

    @Override
    public HealthInfo mutateInstance(HealthInfo originalHealthInfo) {
        Map<String, DiskHealthInfo> diskHealthInfoMap = originalHealthInfo.diskInfoByNode();
        Map<String, DiskHealthInfo> diskHealthInfoMapCopy = new HashMap<>(diskHealthInfoMap);
        if (diskHealthInfoMap.isEmpty()) {
            diskHealthInfoMapCopy.put(
                randomAlphaOfLength(10),
                new DiskHealthInfo(randomFrom(HealthStatus.values()), randomFrom(DiskHealthInfo.Cause.values()))
            );
        } else {
            switch (randomIntBetween(1, 3)) {
                case 1 -> {
                    diskHealthInfoMapCopy.put(
                        randomAlphaOfLength(10),
                        new DiskHealthInfo(randomFrom(HealthStatus.values()), randomFrom(DiskHealthInfo.Cause.values()))
                    );
                }
                case 2 -> {
                    String someNode = randomFrom(diskHealthInfoMap.keySet());
                    diskHealthInfoMapCopy.put(
                        someNode,
                        new DiskHealthInfo(
                            randomValueOtherThan(diskHealthInfoMap.get(someNode).healthStatus(), () -> randomFrom(HealthStatus.values())),
                            randomFrom(DiskHealthInfo.Cause.values())
                        )
                    );
                }
                case 3 -> {
                    diskHealthInfoMapCopy.remove(randomFrom(diskHealthInfoMapCopy.keySet()));
                }
                default -> throw new IllegalStateException();
            }
        }
        return new HealthInfo(diskHealthInfoMapCopy);
    }
}
