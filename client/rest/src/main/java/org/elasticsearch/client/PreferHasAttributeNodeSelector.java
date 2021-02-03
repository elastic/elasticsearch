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
 * Both {@link PreferHasAttributeNodeSelector} and {@link HasAttributeNodeSelector} will work the same
 * if there is a {@link Node} with particular attribute in the attributes,
 * but {@link PreferHasAttributeNodeSelector} will select another {@link Node}s even if there is no {@link Node}
 * with particular attribute in the attributes.
 */
public final class PreferHasAttributeNodeSelector implements NodeSelector {
    private final String key;
    private final String value;

    public PreferHasAttributeNodeSelector(String key, String value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public void select(Iterable<Node> nodes) {
        boolean foundAtLeastOne = false;

        for (Node node : nodes) {
            Map<String, List<String>> attributes = node.getAttributes();

            if (attributes == null) {
                continue;
            }

            List<String> values = attributes.get(key);

            if (values == null) {
                continue;
            }

            if (values.contains(value)) {
                foundAtLeastOne = true;
                break;
            }
        }

        if (foundAtLeastOne) {
            Iterator<Node> nodeIterator = nodes.iterator();
            while (nodeIterator.hasNext()) {
                Map<String, List<String>> attributes = nodeIterator.next().getAttributes();

                if (attributes == null) {
                    continue;
                }

                List<String> values = attributes.get(key);

                if (values == null || values.contains(value) == false) {
                    nodeIterator.remove();
                }
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
        PreferHasAttributeNodeSelector that = (PreferHasAttributeNodeSelector) o;
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
