/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

/**
 * Provides an interface for converting an object into another form. This is to provide a way to convert
 * the values of the CounterMap into a more serializable form that can be converted to json to send for telemetry.
 * @param <T> The resulting type to convert into.
 */
public interface Transformable<T> {

    /**
     * Convert the current object into the specified type.
     * @return the new representation of the object
     */
    T transform();
}
