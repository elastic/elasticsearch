/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.completion.context;

/**
 * Builder for {@link ContextMapping}
 */
public abstract class ContextBuilder<E extends ContextMapping<?>> {

    protected String name;

    /**
     * @param name of the context mapper to build
     */
    protected ContextBuilder(String name) {
        this.name = name;
    }

    public abstract E build();

    /**
     * Create a new {@link GeoContextMapping}
     */
    public static GeoContextMapping.Builder geo(String name) {
        return new GeoContextMapping.Builder(name);
    }

    /**
     * Create a new {@link CategoryContextMapping}
     */
    public static CategoryContextMapping.Builder category(String name) {
        return new CategoryContextMapping.Builder(name);
    }

}
