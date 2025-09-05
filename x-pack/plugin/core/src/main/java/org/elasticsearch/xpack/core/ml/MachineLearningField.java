/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.hash.MurmurHash3;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;

import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Collectors;

public final class MachineLearningField {

    public static final Setting<Boolean> AUTODETECT_PROCESS = Setting.boolSetting(
        "xpack.ml.autodetect_process",
        true,
        Setting.Property.NodeScope
    );
    public static final Setting<ByteSizeValue> MAX_MODEL_MEMORY_LIMIT = Setting.memorySizeSetting(
        "xpack.ml.max_model_memory_limit",
        ByteSizeValue.ZERO,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> MAX_LAZY_ML_NODES = Setting.intSetting(
        "xpack.ml.max_lazy_ml_nodes",
        0,
        0,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    /**
     * This boolean value indicates if `max_machine_memory_percent` should be ignored and an automatic calculation is used instead.
     *
     * This calculation takes into account total node size and the size of the JVM on that node.
     *
     * If the calculation fails, we fall back to `max_machine_memory_percent`.
     */
    public static final Setting<Boolean> USE_AUTO_MACHINE_MEMORY_PERCENT = Setting.boolSetting(
        "xpack.ml.use_auto_machine_memory_percent",
        false,
        Setting.Property.OperatorDynamic,
        Setting.Property.NodeScope
    );

    public static final TimeValue STATE_PERSIST_RESTORE_TIMEOUT = TimeValue.timeValueMinutes(30);
    public static final String ML_FEATURE_FAMILY = "machine-learning";
    public static final LicensedFeature.Momentary ML_API_FEATURE = LicensedFeature.momentary(
        ML_FEATURE_FAMILY,
        "api",
        License.OperationMode.PLATINUM
    );

    // This is the last version when we changed the ML job snapshot format.
    public static final MlConfigVersion MIN_SUPPORTED_SNAPSHOT_VERSION = MlConfigVersion.V_8_3_0;

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
