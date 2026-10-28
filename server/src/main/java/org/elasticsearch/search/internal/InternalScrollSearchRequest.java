/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;
import java.util.Map;

public class InternalScrollSearchRequest extends AbstractTransportRequest {

    private ShardSearchContextId contextId;

    private TimeValue scroll;

    public InternalScrollSearchRequest() {}

    public InternalScrollSearchRequest(SearchScrollRequest request, ShardSearchContextId contextId) {
        this.contextId = contextId;
        this.scroll = request.scroll();
    }

    public InternalScrollSearchRequest(StreamInput in) throws IOException {
        super(in);
        contextId = new ShardSearchContextId(in);
        scroll = in.readOptionalTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        contextId.writeTo(out);
        out.writeOptionalTimeValue(scroll);
    }

    public ShardSearchContextId contextId() {
        return contextId;
    }

    public TimeValue scroll() {
        return scroll;
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
