/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle;

import java.util.List;

public abstract class AbstractLazyPropertyCollection {

    final String name;
    final Object owner;

    public AbstractLazyPropertyCollection(String name) {
        this(name, null);
    }

    public AbstractLazyPropertyCollection(String name, Object owner) {
        this.name = name;
        this.owner = owner;
    }

    public abstract List<? extends Object> getNormalizedCollection();

    void assertNotNull(Object value, String description) {
        if (value == null) {
            throw new NullPointerException(name + " " + description + " was null" + (owner != null ? " when configuring " + owner : ""));
        }
    }

}
