/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.steps;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;

import java.util.Locale;

public interface DataFrameAnalyticsStep {

    enum Name {
        REINDEXING, ANALYSIS, INFERENCE, FINAL;

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    Name name();

    void execute(ActionListener<StepResponse> listener);

    void cancel(String reason, TimeValue timeout);

    void updateProgress(ActionListener<Void> listener);
}
