/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;

import java.util.Objects;
import java.util.function.LongSupplier;

public class PhaseAfterStep extends ClusterStateWaitStep {
    public static final String NAME = "index_age";
    private final TimeValue indexAge;
    private final LongSupplier nowSupplier;

    PhaseAfterStep(LongSupplier nowSupplier, TimeValue indexAge, StepKey key, StepKey nextStepKey) {
        super(key, nextStepKey);
        this.nowSupplier = nowSupplier;
        this.indexAge = indexAge;
    }

    @Override
    public Result isConditionMet(Index index, ClusterState clusterState) {
        IndexMetaData indexMetaData = clusterState.metaData().index(index);
        long lifecycleDate = indexMetaData.getSettings()
            .getAsLong(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, -1L);
        return new Result(nowSupplier.getAsLong() >= lifecycleDate + indexAge.getMillis(), null);
    }

    TimeValue getIndexAge() {
        return indexAge;
    }

    LongSupplier getNowSupplier() {
        return nowSupplier;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), indexAge);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PhaseAfterStep other = (PhaseAfterStep) obj;
        return super.equals(obj) &&
                Objects.equals(indexAge, other.indexAge);
    }
}
