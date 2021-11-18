/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tracing;

/**
 * Represents a distributed tracing system that keeps track of the start and end of various activities in the cluster.
 */
public interface Tracer {

    /**
     * Called when the {@link Traceable} activity starts.
     */
    void onTraceStarted(Traceable traceable);

    /**
     * Called when the {@link Traceable} activity ends.
     */
    void onTraceStopped(Traceable traceable);
}
