/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.action.EmptyResponseListener;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContent;

/**
 * Base class for responses to action requests.
 */
public abstract class ActionResponse extends TransportResponse {

    public ActionResponse() {}

    /**
     * A response with no payload. This is deliberately not an implementation of {@link ToXContent} or similar because an empty response
     * has no valid {@link XContent} representation. Use {@link EmptyResponseListener} to convert this to a valid (plain-text) REST
     * response instead.
     */
    public static final class Empty extends ActionResponse {

        private Empty() { /* singleton */ }

        public static final ActionResponse.Empty INSTANCE = new ActionResponse.Empty();

        @Override
        public String toString() {
            return "ActionResponse.Empty{}";
        }

        @Override
        public void writeTo(StreamOutput out) {}
    }
}
