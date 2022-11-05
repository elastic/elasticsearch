/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
