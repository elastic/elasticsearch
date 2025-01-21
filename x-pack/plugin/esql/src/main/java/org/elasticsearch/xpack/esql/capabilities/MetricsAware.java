/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import java.util.Locale;

/**
 * Interface for plan nodes that need to be accounted in the statistics
 */
public interface MetricsAware {

    /**
     * @return the name of the metric. Only needs to be overwriten if the metric name doesn't match the class name.
     */
    default String metricName() {
        return getClass().getSimpleName().toUpperCase(Locale.ROOT);
    }
}
