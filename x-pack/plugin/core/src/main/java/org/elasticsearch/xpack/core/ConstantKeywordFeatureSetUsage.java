/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.flattened.FlattenedFeatureSetUsage;

import java.io.IOException;
import java.util.Objects;

public class ConstantKeywordFeatureSetUsage extends XPackFeatureSet.Usage {

    public ConstantKeywordFeatureSetUsage(StreamInput input) throws IOException {
        super(input);
    }

    public ConstantKeywordFeatureSetUsage(boolean available, boolean enabled) {
        super(XPackField.CONSTANT_KEYWORD, available, enabled);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlattenedFeatureSetUsage that = (FlattenedFeatureSetUsage) o;
        return available == that.available && enabled == that.enabled;
    }

    @Override
    public int hashCode() {
        return Objects.hash(available, enabled);
    }

}
