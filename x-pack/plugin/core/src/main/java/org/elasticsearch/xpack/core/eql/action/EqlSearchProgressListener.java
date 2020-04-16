/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.eql.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;

public abstract class EqlSearchProgressListener {
    private static final Logger logger = LogManager.getLogger(EqlSearchProgressListener.class);

    public static final EqlSearchProgressListener NOOP = new EqlSearchProgressListener() {
    };

    /**
     * Executed when initial EQL plan is created before the first search.
     */
    protected void onPreAnalyze() {

    }

    /**
     * Executed when initial EQL plan is created before the first search.
     */
    protected void onPlanCreated() {

    }

    /**
     * Executed when a partial search results are obtained.
     *
     * @param response The partial result.
     */
    protected void onPartialResult(EqlSearchResponse response) {
    }

    public final void notifyPreAnalyze() {
        try {
            onPreAnalyze();
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("Failed to execute progress listener on plan created"), e);
        }
    }

    public final void notifyPlanCreated() {
        try {
            onPlanCreated();
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("Failed to execute progress listener on plan created"), e);
        }
    }

    public final void notifyPartialResult(EqlSearchResponse response) {
        try {
            onPartialResult(response);
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("Failed to execute progress listener on partial reduce"), e);
        }
    }
}
