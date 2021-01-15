/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ml.dataframe.steps;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.unit.TimeValue;

import java.util.Locale;

public interface DataFrameAnalyticsStep {

    enum Name {
        REINDEXING, ANALYSIS;

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
