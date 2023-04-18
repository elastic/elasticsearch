/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.desirednodes;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;

import java.io.IOException;

public class DeleteDesiredNodesAction extends ActionType<ActionResponse.Empty> {
    public static final DeleteDesiredNodesAction INSTANCE = new DeleteDesiredNodesAction();
    public static final String NAME = "cluster:admin/desired_nodes/delete";

    DeleteDesiredNodesAction() {
        super(NAME, in -> ActionResponse.Empty.INSTANCE);
    }

    public static class Request extends AcknowledgedRequest<Request> {
        public Request() {}

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }
}
