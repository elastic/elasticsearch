/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.util.Objects;
import java.util.function.Predicate;

/**
 * Fluent builder for a root {@link MapperMergeContext} in tests. Only the {@link MergeReason} is required.
 */
public class TestRootMapperMergeContextBuilder {
    private final MergeReason mergeReason;
    private boolean isSourceSynthetic = false;
    private boolean isDataStream = false;
    private long newFieldsBudget = Long.MAX_VALUE;
    private long nestedFieldsLimit = Long.MAX_VALUE;
    private boolean isStrictColumnar = false;
    private boolean isSourceColumnarStored = false;
    private Predicate<NodeFeature> clusterSupportsFeature = f -> true;

    public TestRootMapperMergeContextBuilder(MergeReason mergeReason) {
        this.mergeReason = Objects.requireNonNull(mergeReason);
    }

    public TestRootMapperMergeContextBuilder sourceSynthetic(boolean isSourceSynthetic) {
        this.isSourceSynthetic = isSourceSynthetic;
        return this;
    }

    public TestRootMapperMergeContextBuilder dataStream(boolean isDataStream) {
        this.isDataStream = isDataStream;
        return this;
    }

    public TestRootMapperMergeContextBuilder newFieldsBudget(long newFieldsBudget) {
        this.newFieldsBudget = newFieldsBudget;
        return this;
    }

    public TestRootMapperMergeContextBuilder nestedFieldsLimit(long nestedFieldsLimit) {
        this.nestedFieldsLimit = nestedFieldsLimit;
        return this;
    }

    public TestRootMapperMergeContextBuilder strictColumnar(boolean isStrictColumnar) {
        this.isStrictColumnar = isStrictColumnar;
        return this;
    }

    public TestRootMapperMergeContextBuilder sourceColumnarStored(boolean isSourceColumnarStored) {
        this.isSourceColumnarStored = isSourceColumnarStored;
        return this;
    }

    public TestRootMapperMergeContextBuilder clusterSupportsFeature(Predicate<NodeFeature> clusterSupportsFeature) {
        this.clusterSupportsFeature = Objects.requireNonNull(clusterSupportsFeature);
        return this;
    }

    public MapperMergeContext build() {
        return MapperMergeContext.root(
            isSourceSynthetic,
            isDataStream,
            mergeReason,
            ParseFieldLimits.forMerge(nestedFieldsLimit, 0, NewFieldsBudget.dropping(newFieldsBudget)),
            isStrictColumnar,
            isSourceColumnarStored,
            clusterSupportsFeature
        );
    }
}
