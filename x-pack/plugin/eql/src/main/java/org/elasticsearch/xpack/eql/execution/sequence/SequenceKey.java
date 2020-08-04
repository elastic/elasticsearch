/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.sequence;

import org.elasticsearch.common.util.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class SequenceKey {

    public static final SequenceKey NONE = new SequenceKey();

    private final Object[] keys;
    private final int hashCode;

    public SequenceKey(Object... keys) {
        this.keys = keys;
        this.hashCode = Objects.hash(keys);
    }

    public List<String> asStringList() {
        String[] s = new String[keys.length];
        for (int i = 0; i < keys.length; i++) {
            s[i] = Objects.toString(keys[i]);
        }
        return Arrays.asList(s);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SequenceKey other = (SequenceKey) obj;
        return Arrays.deepEquals(keys, other.keys);
    }

    @Override
    public String toString() {
        return CollectionUtils.isEmpty(keys) ? "NONE" : Arrays.toString(keys);
    }
}
