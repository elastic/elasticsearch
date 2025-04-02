/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.xpack.core.ml.aggs.changepoint.ChangePointDetector;

public record MlServices(ChangePointDetector changePointDetector) {
    @Inject
    public MlServices {
    }
}
