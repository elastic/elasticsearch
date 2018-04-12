/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.Index;

import java.util.Objects;

public class ShrunkenIndexCheckStep extends ClusterStateWaitStep {
    public static final String NAME = "is-shrunken-index";
    private String shrunkIndexPrefix;

    public ShrunkenIndexCheckStep(StepKey key, StepKey nextStepKey, String shrunkIndexPrefix) {
        super(key, nextStepKey);
        this.shrunkIndexPrefix = shrunkIndexPrefix;
    }

    String getShrunkIndexPrefix() {
        return shrunkIndexPrefix;
    }

    @Override
    public boolean isConditionMet(Index index, ClusterState clusterState) {
        String shrunkenIndexSource = IndexMetaData.INDEX_SHRINK_SOURCE_NAME.get(
            clusterState.metaData().index(index).getSettings());
        if (Strings.isNullOrEmpty(shrunkenIndexSource)) {
            throw new IllegalStateException("step[" + NAME + "] is checking an un-shrunken index[" + index.getName() + "]");
        }
        return index.getName().equals(shrunkIndexPrefix + shrunkenIndexSource) &&
                clusterState.metaData().index(shrunkenIndexSource) == null;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), shrunkIndexPrefix);
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ShrunkenIndexCheckStep other = (ShrunkenIndexCheckStep) obj;
        return super.equals(obj) && 
                Objects.equals(shrunkIndexPrefix, other.shrunkIndexPrefix);
    }
}
