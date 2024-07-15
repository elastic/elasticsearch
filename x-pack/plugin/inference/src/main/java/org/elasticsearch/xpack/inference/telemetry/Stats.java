/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.telemetry;

import org.elasticsearch.xpack.core.inference.SerializableStats;

public interface Stats {

    /**
     * Increase the counter by one.
     */
    void increment();

    /**
     * Return the current value of the counter.
     * @return the current value of the counter
     */
    long getCount();

    /**
     * Convert the object into a serializable form that can be written across nodes and returned in xcontent format.
     * @return the serializable format of the object
     */
    SerializableStats toSerializableForm();
}
