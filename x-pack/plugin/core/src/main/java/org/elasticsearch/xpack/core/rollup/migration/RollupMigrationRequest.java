/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.rollup.migration;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.reindex.AbstractBulkIndexByScrollRequest;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class RollupMigrationRequest extends AbstractBulkIndexByScrollRequest<RollupMigrationRequest> {
    private final IndexRequest destination;

    public RollupMigrationRequest(StreamInput in) throws IOException {
        super(in);
        destination = new IndexRequest(in);
    }

    protected RollupMigrationRequest(SearchRequest searchRequest, IndexRequest destination, boolean setDefaults) {
        super(searchRequest, setDefaults);
        this.destination = destination;
    }

    public IndexRequest getDestination() {
        return destination;
    }

    @Override
    protected RollupMigrationRequest self() {
        return this;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = super.validate();
        if (getSearchRequest().indices().length != 1) {
            e = addValidationError("Can only migrate a single rollup index at a time", e);
        }

        // Is this the right place to check that the index is a v1 rollup that we can migrate?
        // How do we get index metadata here?
        return e;
    }

    @Override
    public RollupMigrationRequest forSlice(TaskId slicingTask, SearchRequest slice, int totalSlices) {
        throw new UnsupportedOperationException();
    }
}
