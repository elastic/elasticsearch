/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        return Objects.equals(key, that.key) && Objects.equals(value, that.value);
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
