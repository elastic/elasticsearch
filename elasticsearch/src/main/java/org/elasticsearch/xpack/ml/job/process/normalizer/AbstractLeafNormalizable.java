/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import java.util.Collections;
import java.util.List;

abstract class AbstractLeafNormalizable implements Normalizable {
    @Override
    public final boolean isContainerOnly() {
        return false;
    }

    @Override
    public final List<Integer> getChildrenTypes() {
        return Collections.emptyList();
    }

    @Override
    public final List<Normalizable> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public final List<Normalizable> getChildren(int type) {
        throw new IllegalStateException(getClass().getSimpleName() + " has no children");
    }

    @Override
    public final boolean setMaxChildrenScore(int childrenType, double maxScore) {
        throw new IllegalStateException(getClass().getSimpleName() + " has no children");
    }
}
