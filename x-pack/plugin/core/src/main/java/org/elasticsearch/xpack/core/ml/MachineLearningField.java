/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public final class MachineLearningField {
    public static final Setting<Boolean> AUTODETECT_PROCESS =
            Setting.boolSetting("xpack.ml.autodetect_process", true, Setting.Property.NodeScope);
    public static final Setting<ByteSizeValue> MAX_MODEL_MEMORY_LIMIT =
            Setting.memorySizeSetting("xpack.ml.max_model_memory_limit", ByteSizeValue.ZERO,
                    Setting.Property.Dynamic, Setting.Property.NodeScope);
    public static final TimeValue STATE_PERSIST_RESTORE_TIMEOUT = TimeValue.timeValueMinutes(30);

    private MachineLearningField() {}

}
