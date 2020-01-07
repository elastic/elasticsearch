/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public final class MachineLearningField {
    public static final Setting<Boolean> AUTODETECT_PROCESS =
            Setting.boolSetting("xpack.ml.autodetect_process", true, Setting.Property.NodeScope);
    public static final Setting<ByteSizeValue> MAX_MODEL_MEMORY_LIMIT =
            Setting.memorySizeSetting("xpack.ml.max_model_memory_limit", ByteSizeValue.ZERO,
                    Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final TimeValue STATE_PERSIST_RESTORE_TIMEOUT = TimeValue.timeValueMinutes(30);

    private MachineLearningField() {}

    public static String valuesToId(String... values) {
        String combined = Arrays.stream(values).filter(Objects::nonNull).collect(Collectors.joining());
        byte[] bytes = combined.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());
        byte[] hashedBytes = new byte[16];
        System.arraycopy(Numbers.longToBytes(hash.h1), 0, hashedBytes, 0, 8);
        System.arraycopy(Numbers.longToBytes(hash.h2), 0, hashedBytes, 8, 8);
        return new BigInteger(hashedBytes) + "_" + combined.length();
    }
}
