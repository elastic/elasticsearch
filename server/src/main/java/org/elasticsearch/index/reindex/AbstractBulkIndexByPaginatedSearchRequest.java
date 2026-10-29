/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.TruncatedAppender;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.Script;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

public abstract class AbstractBulkIndexByPaginatedSearchRequest<Self extends AbstractBulkIndexByPaginatedSearchRequest<Self>> extends
    AbstractBulkByPaginatedSearchRequest<Self> {
    /**
     * The most characters of the script params that are included in the task description.
     */
    static final int MAX_DESCRIPTION_PARAMS_LENGTH = 1024;

    /**
     * Script to modify the documents before they are processed.
     */
    private Script script;

    public AbstractBulkIndexByPaginatedSearchRequest(StreamInput in) throws IOException {
        super(in);
        if (in.readBoolean()) {
            script = new Script(in);
        }
    }

    /**
     * Constructor for actual use.
     *
     * @param searchRequest the search request to execute to get the documents to process
     * @param setDefaults should this request set the defaults on the search request? Usually set to true but leave it false to support
     *        request slicing
     */
    protected AbstractBulkIndexByPaginatedSearchRequest(SearchRequest searchRequest, boolean setDefaults) {
        super(searchRequest, setDefaults);
    }

    /**
     * Script to modify the documents before they are processed.
     */
    public Script getScript() {
        return script;
    }

    /**
     * Script to modify the documents before they are processed.
     */
    public Self setScript(@Nullable Script script) {
        this.script = script;
        return self();
    }

    @Override
    protected Self doForSlice(Self request, TaskId slicingTask, int totalSlices, int activeSlices) {
        return super.doForSlice(request, slicingTask, totalSlices, activeSlices).setScript(script);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(script);
    }

    @Override
    protected void searchToString(StringBuilder b) {
        super.searchToString(b);
        if (script != null) {
            b.append(" updated with ");
            appendScript(b, script);
        }
    }

    /**
     * Appends the same text as {@link Script#toString()}, but cuts the params after {@link #MAX_DESCRIPTION_PARAMS_LENGTH} characters.
     * Unlike the script source, the params are unbounded, and the task description is kept for the life of the task.
     */
    private static void appendScript(StringBuilder b, Script script) {
        b.append("Script{type=")
            .append(script.getType())
            .append(", lang='")
            .append(script.getLang())
            .append("', idOrCode='")
            .append(script.getIdOrCode())
            .append("', options=")
            .append(script.getOptions())
            .append(", params=");
        if (TruncatedAppender.append(b, script.getParams(), MAX_DESCRIPTION_PARAMS_LENGTH) == false) {
            b.append("...");
        }
        b.append('}');
    }
}
