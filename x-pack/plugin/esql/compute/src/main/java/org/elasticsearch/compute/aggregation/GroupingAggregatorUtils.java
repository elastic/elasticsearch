/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.elasticsearch.compute.data.Vector;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;

public class GroupingAggregatorUtils {

    private GroupingAggregatorUtils() {}

    /** Releases any vectors that are releasable - big array wrappers. */
    public static void releaseVectors(DriverContext driverContext, Vector... vectors) {
        for (var vector : vectors) {
            if (vector instanceof Releasable releasable) {
                IOUtils.closeWhileHandlingException(releasable);
                driverContext.removeReleasable(releasable);
            }
        }
    }
}
