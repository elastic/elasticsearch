/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.master.AcknowledgedRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Objects;

public class DeleteCCMConfigurationAction extends ActionType<CCMEnabledActionResponse> {

    public static final DeleteCCMConfigurationAction INSTANCE = new DeleteCCMConfigurationAction();
    public static final String NAME = "cluster:admin/xpack/inference/ccm/delete";

    public DeleteCCMConfigurationAction() {
        super(NAME);
    }

    public static class Request extends AcknowledgedRequest<Request> {

        public Request(TimeValue masterNodeTimeout, TimeValue ackTimeout) {
            super(masterNodeTimeout, ackTimeout);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            return true;
        }

        @Override
        public int hashCode() {
            // The class doesn't have any members at the moment so return the same hash code
            return Objects.hash(NAME);
        }
    }
}
