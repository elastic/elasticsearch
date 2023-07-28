/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.recovery;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.broadcast.BroadcastRequest;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;

/**
 * Request for recovery information
 */
public class RecoveryRequest extends BroadcastRequest<RecoveryRequest> {

    private boolean detailed = false;       // Provides extra details in the response
    private boolean activeOnly = false;     // Only reports on active recoveries

    /**
     * Constructs a request for recovery information for all shards
     */
    public RecoveryRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public RecoveryRequest(StreamInput in) throws IOException {
        super(in);
        detailed = in.readBoolean();
        activeOnly = in.readBoolean();
    }

    /**
     * Constructs a request for recovery information for all shards for the given indices
     *
     * @param indices   Comma-separated list of indices about which to gather recovery information
     */
    public RecoveryRequest(String... indices) {
        super(indices, IndicesOptions.STRICT_EXPAND_OPEN_CLOSED);
    }

    /**
     * Set value of the detailed flag. Detailed requests will contain extra
     * information such as a list of physical files and their recovery progress.
     *
     * @param detailed  Whether or not to set the detailed flag
     */
    public void detailed(boolean detailed) {
        this.detailed = detailed;
    }

    /**
     * True if activeOnly flag is set, false otherwise. This value is false by default.
     *
     * @return  True if activeOnly flag is set, false otherwise
     */
    public boolean activeOnly() {
        return activeOnly;
    }

    /**
     * Set value of the activeOnly flag. If true, this request will only response with
     * on-going recovery information.
     *
     * @param activeOnly    Whether or not to set the activeOnly flag.
     */
    public void activeOnly(boolean activeOnly) {
        this.activeOnly = activeOnly;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(detailed);
        out.writeBoolean(activeOnly);
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers);
    }
}
