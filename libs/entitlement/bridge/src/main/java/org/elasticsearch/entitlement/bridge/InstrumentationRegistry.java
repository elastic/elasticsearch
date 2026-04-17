/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.bridge;

public interface InstrumentationRegistry {

    void check$(String instrumentationId, Class<?> callingClass, Object... args) throws Exception;

    /**
     * Returns the configured default reference value for the given instrumentation id.
     * Called from bytecode catch handlers when a reference-type default is needed on denial.
     */
    Object defaultValue$(String instrumentationId);
}
