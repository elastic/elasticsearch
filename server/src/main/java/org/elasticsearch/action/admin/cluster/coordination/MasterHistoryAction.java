/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.cluster.coordination.MasterHistoryService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * This action is used to fetch the MasterHistory from a remote node.
 */
public class MasterHistoryAction extends ActionType<MasterHistoryAction.Response> {

    public static final MasterHistoryAction INSTANCE = new MasterHistoryAction();
    public static final String NAME = "internal:cluster/master_history/get";

    private MasterHistoryAction() {
        super(NAME, MasterHistoryAction.Response::new);
    }

    public static class Request extends ActionRequest {

        public Request() {}

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            // There are no parameters, so all instances of this class are equal
            if (this == o) return true;
            return o != null && getClass() == o.getClass();
        }

        @Override
        public int hashCode() {
            // There are no parameters, so all instances of this class are equal
            return 1;
        }

    }

    public static class Response extends ActionResponse {

        private final List<DiscoveryNode> masterHistory;

        public Response(List<DiscoveryNode> masterHistory) {
            this.masterHistory = masterHistory;
        }

        public Response(StreamInput in) throws IOException {
            int mastersCount = in.readVInt();
            masterHistory = new ArrayList<>(mastersCount);
            for (int i = 0; i < mastersCount; i++) {
                masterHistory.add(in.readOptionalWriteable(DiscoveryNode::new));
            }
        }

        /**
         * Returns an ordered list of DiscoveryNodes that the node responding has seen to be master nodes over the last 30 minutes, ordered
         * oldest first. Note that these DiscoveryNodes can be null.
         * @return a list of DiscoveryNodes that the node responding has seen to be master nodes over the last 30 minutes, ordered oldest
         * first
         */
        public List<DiscoveryNode> getMasterHistory() {
            return masterHistory;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(masterHistory.size());
            for (DiscoveryNode master : masterHistory) {
                out.writeOptionalWriteable(master);
            }
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            MasterHistoryAction.Response response = (MasterHistoryAction.Response) o;
            return masterHistory.equals(response.masterHistory);
        }

        @Override
        public int hashCode() {
            return Objects.hash(masterHistory);
        }
    }

    /**
     * This transport action fetches the MasterHistory from a remote node.
     */
    public static class TransportAction extends HandledTransportAction<Request, Response> {
        private final MasterHistoryService masterHistoryService;

        @Inject
        public TransportAction(TransportService transportService, ActionFilters actionFilters, MasterHistoryService masterHistoryService) {
            super(MasterHistoryAction.NAME, transportService, actionFilters, MasterHistoryAction.Request::new);
            this.masterHistoryService = masterHistoryService;
        }

        @Override
        protected void doExecute(Task task, MasterHistoryAction.Request request, ActionListener<Response> listener) {
            listener.onResponse(new MasterHistoryAction.Response(masterHistoryService.getLocalMasterHistory().getNodes()));
        }
    }

}
