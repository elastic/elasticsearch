/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexing;

import org.elasticsearch.action.index.IndexRequest;

import java.util.List;

public class Iteration<JobPosition> {

    private final boolean isDone;

    private final JobPosition position;

    private final List<IndexRequest> toIndex;

    public Iteration(List<IndexRequest> toIndex, JobPosition position, boolean isDone) {
        this.toIndex = toIndex;
        this.position = position;
        this.isDone = isDone;
    }

    public boolean isDone() {
        return isDone;
    }

    public JobPosition getPosition() {
        return position;
    }

    public List<IndexRequest> getToIndex() {
        return toIndex;
    }

}
