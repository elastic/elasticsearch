/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.local;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class LocalClusterStateHelper extends ESTestCase {

    /**
     * This class can be used to test the BwC serialization of action requests that are transitioning from
     * {@link org.elasticsearch.action.support.master.MasterNodeReadRequest} or
     * {@link org.elasticsearch.action.support.master.MasterNodeRequest} to {@link LocalClusterStateRequest}. These request classes no
     * longer need to serialize to the wire, but do need to be able to deserialize incoming requests from the wire for maintaining BwC,
     * as old nodes might still forward a request to a new (master) node.
     * <p>
     * This class takes care of filling in for the wire serialization that the master-request superclasses would do and implementing
     * the equals and hashcode which are required for the serialization tests.
     */
    public abstract static class RequestSerializationWrapper<Request> extends ActionRequest {

        private final Request request;
        private final boolean writeLocal;

        public RequestSerializationWrapper(Request request) {
            this(request, true);
        }

        public RequestSerializationWrapper(Request request, boolean writeLocal) {
            this.request = request;
            this.writeLocal = writeLocal;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            // We write random values according to what MasterNodeRequest and MasterNodeReadRequest would do.
            out.writeTimeValue(randomPositiveTimeValue());
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                out.writeVLong(randomNonNegativeLong());
            }
            // Writing this boolean is optional because only MasterNodeReadRequest writes a boolean, MasterNodeRequest does not.
            if (writeLocal) {
                out.writeBoolean(randomBoolean());
            }
        }

        @Override
        public final ActionRequestValidationException validate() {
            return null;
        }

        public Request request() {
            return request;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RequestSerializationWrapper<?> requestWrapper = (RequestSerializationWrapper<?>) o;
            return request.equals(requestWrapper.request);
        }

        @Override
        public int hashCode() {
            return request.hashCode();
        }
    }

    /**
     * This class can be used to test the BwC serialization of action responses that are transitioning from
     * {@link org.elasticsearch.action.support.master.MasterNodeReadRequest} or
     * {@link org.elasticsearch.action.support.master.MasterNodeRequest} to {@link LocalClusterStateRequest}. These response classes no
     * longer need to deserialize from the wire, but do need to be able to serialize outgoing responses to the wire for maintaining BwC,
     * as old nodes might still forward a request to a new (master) node and we thus need to be able to send the response back.
     * <p>
     * This class takes care of implementing the equals and hashcode which are required for the serialization tests.
     */
    public abstract static class ResponseSerializationWrapper<Response extends Writeable> implements Writeable {

        private final Response response;

        public ResponseSerializationWrapper(Response response) {
            this.response = response;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            response.writeTo(out);
        }

        public Response response() {
            return response;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResponseSerializationWrapper<?> responseWrapper = (ResponseSerializationWrapper<?>) o;
            return response.equals(responseWrapper.response);
        }

        @Override
        public int hashCode() {
            return response.hashCode();
        }
    }
}
