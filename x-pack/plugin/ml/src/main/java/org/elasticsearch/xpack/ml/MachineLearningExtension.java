/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.elasticsearch.common.settings.Settings;

public interface MachineLearningExtension {

    default void configure(Settings settings) {}

    boolean useIlm();

    boolean includeNodeInfo();

    boolean isAnomalyDetectionEnabled();

    boolean isDataFrameAnalyticsEnabled();

    boolean isNlpEnabled();
}
