/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.tracing;

/**
 * A class that can be traced using the telemetry tracing API
 */
public interface Traceable {
    /**
     * A consistent id for the span.  Should be structured "[short-name]-[unique-id]" ie "request-abc1234"
     */
    String getSpanId();
}
