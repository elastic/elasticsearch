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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

public abstract class AbstractBulkIndexByScrollRequest<Self extends AbstractBulkIndexByScrollRequest<Self>> extends
    AbstractBulkByScrollRequest<Self> {
    /**
     * Script to modify the documents before they are processed.
     */
    private Script script;

    public AbstractBulkIndexByScrollRequest(StreamInput in) throws IOException {
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
    protected AbstractBulkIndexByScrollRequest(SearchRequest searchRequest, boolean setDefaults) {
        super(searchRequest, setDefaults);

        SearchSourceBuilder searchSourceBuilder = searchRequest.source();
        if (searchSourceBuilder == null) {
            searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
        }

        FetchSourceContext fetchSourceContext = searchSourceBuilder.fetchSource();
        if (fetchSourceContext == null) {
            // By default, use a fetch source context that gets vector fields so that reindex and update-by-query use existing embeddings
            searchSourceBuilder.fetchSource(FetchSourceContext.FETCH_SOURCE_WITH_VECTORS);
        } else if (fetchSourceContext.includeVectors() == null) {
            // If the user did not set a value for include_vectors, set it to true so that reindex and update-by-query use existing
            // embeddings
            searchSourceBuilder.fetchSource(
                FetchSourceContext.of(fetchSourceContext.fetchSource(), fetchSourceContext.includes(), fetchSourceContext.excludes(), true)
            );
        }
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
    protected Self doForSlice(Self request, TaskId slicingTask, int totalSlices) {
        return super.doForSlice(request, slicingTask, totalSlices).setScript(script);
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
            b.append(" updated with ").append(script);
        }
    }
}
