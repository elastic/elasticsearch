/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tracing;

import java.util.Map;

/**
 * Something which maps onto a <i>span</i> in a distributed trace.
 */
public interface Traceable {

    /**
     * @return a key which uniquely identifies the span.
     */
    String getSpanId();

    /**
     * @return the name of the span as seen by the external tracing system (e.g. the action name for a task)
     */
    String getSpanName();

    /**
     * @return extra metadata about the span.
     */
    Map<String, Object> getAttributes();
}
