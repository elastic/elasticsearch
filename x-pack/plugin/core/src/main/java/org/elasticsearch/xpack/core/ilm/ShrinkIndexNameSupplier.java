/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

public final class ShrinkIndexNameSupplier {

    private ShrinkIndexNameSupplier() {
    }

    /**
     * This could be seen as a getter with a fallback, as it'll attempt to read the shrink index name from the provided lifecycle execution
     * state. If no shrink index name was recorded in the execution state it'll construct the shrink index name by prepending the provided
     * prefix to the source index name.
     */
    static String getShrinkIndexName(String sourceIndexName, LifecycleExecutionState lifecycleState, String fallbackPrefix) {
        String shrunkenIndexName = lifecycleState.getShrinkIndexName();
        if (shrunkenIndexName == null) {
            // this is for BWC reasons for polices that are in the middle of executing the shrink action when the update to generated
            // names happens
            shrunkenIndexName = fallbackPrefix + sourceIndexName;
        }
        return shrunkenIndexName;
    }
}
