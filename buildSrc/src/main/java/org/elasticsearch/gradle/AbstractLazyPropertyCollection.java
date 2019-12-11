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
