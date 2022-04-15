/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.coordination;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.cluster.coordination.MasterHistory;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * This action is used to fetch the MasterHistory from a remote node.
 */
public class MasterHistoryAction extends ActionType<MasterHistoryAction.Response> {

    public static final MasterHistoryAction INSTANCE = new MasterHistoryAction();
    public static final String NAME = "coordination:admin/master_history/get";

    private MasterHistoryAction() {
        super(NAME, MasterHistoryAction.Response::new);
    }

    public static class Request extends MasterNodeReadRequest<MasterHistoryAction.Request> {

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

        private final MasterHistory masterHistory;

        public Response(MasterHistory masterHistory) {
            this.masterHistory = masterHistory;
        }

        public Response(StreamInput in) throws IOException {
            this(new MasterHistory(in));
        }

        public MasterHistory getMasterHistory() {
            return masterHistory;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            masterHistory.writeTo(out);
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

}
