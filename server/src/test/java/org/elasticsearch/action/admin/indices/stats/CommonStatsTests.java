/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.stats;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public class CommonStatsTests extends AbstractWireSerializingTestCase<CommonStats> {
    @Override
    protected Writeable.Reader<CommonStats> instanceReader() {
        return CommonStats::new;
    }

    private CommonStatsFlags randomFlags() {
        int len = randomIntBetween(0, CommonStatsFlags.ALL.getFlags().length);
        Map<Integer, CommonStatsFlags.Flag> flagMap = new HashMap<>();
        for (int i = 0; i < len; i++) {
            CommonStatsFlags.Flag flag = randomFrom(EnumSet.allOf(CommonStatsFlags.Flag.class));
            flagMap.putIfAbsent(flag.ordinal(), flag);
        }
        CommonStatsFlags.Flag[] flags = flagMap.values().toArray(new CommonStatsFlags.Flag[0]);
        return new CommonStatsFlags(flags);
    }

    @Override
    protected CommonStats createTestInstance() {
        if (frequently()) {
            if (randomBoolean()) {
                return new CommonStats(CommonStatsFlags.ALL);
            } else {
                return new CommonStats(CommonStatsFlags.NONE);
            }
        }
        return new CommonStats(randomFlags());
    }

    @Override
    protected CommonStats mutateInstance(CommonStats instance) throws IOException {
        CommonStats another = createTestInstance();
        long denseVectorCount = instance.getDenseVectorStats() == null
            ? randomNonNegativeLong()
            : randomValueOtherThan(instance.getDenseVectorStats().getValueCount(), ESTestCase::randomNonNegativeLong);
        if (another.getDenseVectorStats() == null) {
            another.denseVectorStats = new DenseVectorStats(denseVectorCount);
        } else {
            another.getDenseVectorStats().add(new DenseVectorStats(denseVectorCount));
        }
        another.add(instance);
        return another;
    }
}
