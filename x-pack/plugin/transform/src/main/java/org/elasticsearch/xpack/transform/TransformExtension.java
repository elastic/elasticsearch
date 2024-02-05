/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;

public interface TransformExtension {

    boolean includeNodeInfo();

    Settings getTransformInternalIndexAdditionalSettings();

    /**
     * Provides destination index settings, hardcoded at the moment. In future this might be customizable or generation could be based on
     * source settings.
     */
    Settings getTransformDestinationIndexSettings();

    // TODO(jkuipers): remove this default implementation after the ServerlessTransformPlugin
    // in the elasticsearch-serverless project is updated.
    default TimeValue getMinFrequency() {
        return TimeValue.timeValueSeconds(1);
    }
}
