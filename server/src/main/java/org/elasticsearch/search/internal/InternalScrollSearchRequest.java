/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Map;

public class InternalScrollSearchRequest extends TransportRequest {

    private ShardSearchContextId contextId;

    private Scroll scroll;

    public InternalScrollSearchRequest() {}

    public InternalScrollSearchRequest(SearchScrollRequest request, ShardSearchContextId contextId) {
        this.contextId = contextId;
        this.scroll = request.scroll();
    }

    public InternalScrollSearchRequest(StreamInput in) throws IOException {
        super(in);
        contextId = new ShardSearchContextId(in);
        scroll = in.readOptionalWriteable(Scroll::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        contextId.writeTo(out);
        out.writeOptionalWriteable(scroll);
    }

    public ShardSearchContextId contextId() {
        return contextId;
    }

    public Scroll scroll() {
        return scroll;
    }

    public InternalScrollSearchRequest scroll(Scroll scroll) {
        this.scroll = scroll;
        return this;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchShardTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public String getDescription() {
        return "id[" + contextId.getId() + "], scroll[" + scroll + "]";
    }

}
