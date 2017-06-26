/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.utils;


/**
 * Parameters used by machine learning for controlling X Content serialisation.
 */
public final class ToXContentParams {

    /**
     * Parameter to indicate whether we are serialising to X Content for cluster state output.
     */
    public static final String FOR_CLUSTER_STATE = "for_cluster_state";

    private ToXContentParams() {
    }
}
