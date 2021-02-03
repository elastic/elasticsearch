/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link NodeSelector} that selects nodes that have a particular value
 * for an attribute.
 */
public final class HasAttributeNodeSelector implements NodeSelector {
    private final String key;
    private final String value;

    public HasAttributeNodeSelector(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public void select(Iterable<Node> nodes) {
        Iterator<Node> itr = nodes.iterator();
        while (itr.hasNext()) {
            Map<String, List<String>> allAttributes = itr.next().getAttributes();
            if (allAttributes == null) continue;
            List<String> values = allAttributes.get(key);
            if (values == null || false == values.contains(value)) {
                itr.remove();
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HasAttributeNodeSelector that = (HasAttributeNodeSelector) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value);
    }

    @Override
    public String toString() {
        return key + "=" + value;
    }
}
