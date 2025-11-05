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
 * Base class for all action responses in Elasticsearch. An action response represents the result
 * of executing an action request, containing the data or status information returned by the operation.
 *
 * <p>Concrete action responses should extend this class and implement the necessary methods to
 * serialize/deserialize their state for network transmission. Many responses also implement
 * {@link ToXContent} to provide JSON/XML representations of the response data.
 *
 * <p><b>Usage Examples:</b></p>
 * <pre>{@code
 * // Example of a concrete action response
 * public class MyCustomResponse extends ActionResponse implements ToXContent {
 *     private final String result;
 *
 *     public MyCustomResponse(String result) {
 *         this.result = result;
 *     }
 *
 *     public MyCustomResponse(StreamInput in) throws IOException {
 *         super();
 *         this.result = in.readString();
 *     }
 *
 *     public String getResult() {
 *         return result;
 *     }
 *
 *     @Override
 *     public void writeTo(StreamOutput out) throws IOException {
 *         out.writeString(result);
 *     }
 *
 *     @Override
 *     public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
 *         builder.startObject();
 *         builder.field("result", result);
 *         builder.endObject();
 *         return builder;
 *     }
 * }
 * }</pre>
 */
public abstract class ActionResponse extends TransportResponse {

    /**
     * Constructs a new action response.
     */
    public ActionResponse() {}

    /**
     * A response with no payload. This is deliberately not an implementation of {@link ToXContent} or similar
     * because an empty response has no valid {@link XContent} representation.
     *
     * <p>Use {@link EmptyResponseListener} to convert this to a valid (plain-text) REST response instead.
     * This singleton instance is used when an action completes successfully but has no meaningful data to return.
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * // Return an empty response from an action
     * listener.onResponse(ActionResponse.Empty.INSTANCE);
     *
     * // Check if a response is empty
     * if (response == ActionResponse.Empty.INSTANCE) {
     *     // Handle empty response
     * }
     * }</pre>
     */
    public static final class Empty extends ActionResponse {

        private Empty() { /* singleton */ }

        /**
         * The singleton instance of the empty response.
         */
        public static final ActionResponse.Empty INSTANCE = new ActionResponse.Empty();

        @Override
        public String toString() {
            return "ActionResponse.Empty{}";
        }

        /**
         * Writes this empty response to the stream. Since there is no payload, this method does nothing.
         *
         * @param out the stream output (unused for empty response)
         */
        @Override
        public void writeTo(StreamOutput out) {}
    }
}
