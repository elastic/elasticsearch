/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

/**
 * Contains utilities for working with Java types.
 */
public abstract class Types {

    /**
     * There are some situations where we cannot appease javac's type checking, and we
     * need to forcibly cast an object's type. Please don't use this method unless you
     * have no choice.
     * @param argument the object to cast
     * @param <T> the inferred type to which to cast the argument
     * @return a cast version of the argument
     */
    @SuppressWarnings("unchecked")
    public static <T> T forciblyCast(Object argument) {
        return (T) argument;
    }
}
