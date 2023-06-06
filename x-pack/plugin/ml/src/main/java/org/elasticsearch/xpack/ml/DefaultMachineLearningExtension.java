/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

public class DefaultMachineLearningExtension implements MachineLearningExtension {
    @Override
    public boolean useIlm() {
        return true;
    }

    @Override
    public boolean includeNodeInfo() {
        return true;
    }

    @Override
    public boolean isAnomalyDetectionEnabled() {
        return true;
    }

    @Override
    public boolean isDataFrameAnalyticsEnabled() {
        return true;
    }

    @Override
    public boolean isNlpEnabled() {
        return true;
    }
}
