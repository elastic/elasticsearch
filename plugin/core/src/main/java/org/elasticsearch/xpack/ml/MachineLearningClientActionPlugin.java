/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;

public interface MachineLearningClientActionPlugin {

    Setting<ByteSizeValue> MAX_MODEL_MEMORY_LIMIT =
            Setting.memorySizeSetting("xpack.ml.max_model_memory_limit", new ByteSizeValue(0),
                    Setting.Property.Dynamic, Setting.Property.NodeScope);

    TimeValue STATE_PERSIST_RESTORE_TIMEOUT = TimeValue.timeValueMinutes(30);
}
