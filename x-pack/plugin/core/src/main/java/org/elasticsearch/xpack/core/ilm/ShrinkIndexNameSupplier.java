/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.metadata.LifecycleExecutionState;

public final class ShrinkIndexNameSupplier {

    public static final String SHRUNKEN_INDEX_PREFIX = "shrink-";

    private ShrinkIndexNameSupplier() {}

    /**
     * This could be seen as a getter with a fallback, as it'll attempt to read the shrink index name from the provided lifecycle execution
     * state. If no shrink index name was recorded in the execution state it'll construct the shrink index name by prepending
     * {@link #SHRUNKEN_INDEX_PREFIX} to the source index name.
     */
    static String getShrinkIndexName(String sourceIndexName, LifecycleExecutionState lifecycleState) {
        String shrunkenIndexName = lifecycleState.shrinkIndexName();
        if (shrunkenIndexName == null) {
            // this is for BWC reasons for polices that are in the middle of executing the shrink action when the update to generated
            // names happens
            shrunkenIndexName = SHRUNKEN_INDEX_PREFIX + sourceIndexName;
        }
        return shrunkenIndexName;
    }
}
