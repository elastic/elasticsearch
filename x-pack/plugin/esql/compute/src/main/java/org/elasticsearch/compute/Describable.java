/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute;

/**
 * A component capable of describing itself.
 */
public interface Describable {

    /**
     * Returns a description of the component. This description can be more specific than Object::toString.
     *
     * @return the description
     */
    String describe();
}
