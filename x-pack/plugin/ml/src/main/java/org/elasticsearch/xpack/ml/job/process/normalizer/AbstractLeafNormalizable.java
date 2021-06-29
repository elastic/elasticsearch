/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import java.util.Collections;
import java.util.List;

abstract class AbstractLeafNormalizable extends Normalizable {

    AbstractLeafNormalizable(String indexName) {
        super(indexName);
    }

    @Override
    public final boolean isContainerOnly() {
        return false;
    }

    @Override
    public final List<ChildType> getChildrenTypes() {
        return Collections.emptyList();
    }

    @Override
    public final List<Normalizable> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public final List<Normalizable> getChildren(ChildType type) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " has no children");
    }

    @Override
    public final boolean setMaxChildrenScore(ChildType childrenType, double maxScore) {
        throw new UnsupportedOperationException(getClass().getSimpleName() + " has no children");
    }
}
