/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import java.util.Comparator;

/**
 * {@link Comparator}-related utility methods.
 */
public enum Comparators {
    ;

    /**
     * Compare <code>d1</code> against <code>d2</code>, pushing {@value Double#NaN} at the bottom.
     */
    public static int compareDiscardNaN(double d1, double d2, boolean asc) {
        if (Double.isNaN(d1)) {
            return Double.isNaN(d2) ? 0 : 1;
        } else if (Double.isNaN(d2)) {
            return -1;
        } else {
            return asc ? Double.compare(d1, d2) : Double.compare(d2, d1);
        }
    }

}
