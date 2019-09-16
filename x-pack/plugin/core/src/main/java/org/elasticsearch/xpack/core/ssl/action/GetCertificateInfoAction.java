/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Action to obtain information about X.509 (SSL/TLS) certificates that are being used by X-Pack.
 * The primary use case is for tracking the expiry dates of certificates.
 */
public class GetCertificateInfoAction extends ActionType<GetCertificateInfoAction.Response> {

    public static final GetCertificateInfoAction INSTANCE = new GetCertificateInfoAction();
    public static final String NAME = "cluster:monitor/xpack/ssl/certificates/get";

    private GetCertificateInfoAction() {
        super(NAME, GetCertificateInfoAction.Response::new);
    }

    public static class Request extends ActionRequest {

        Request() {}

        Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private Collection<CertificateInfo> certificates;

        public Response(StreamInput in) throws IOException {
            super(in);
            this.certificates = new ArrayList<>();
            int count = in.readVInt();
            for (int i = 0; i < count; i++) {
                certificates.add(new CertificateInfo(in));
            }
        }

        public Response(Collection<CertificateInfo> certificates) {
            this.certificates = certificates;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startArray();
            for (CertificateInfo cert : certificates) {
                cert.toXContent(builder, params);
            }
            return builder.endArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(certificates.size());
            for (CertificateInfo cert : certificates) {
                cert.writeTo(out);
            }
        }

        }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response> {

        public RequestBuilder(ElasticsearchClient client, GetCertificateInfoAction action) {
            super(client, action, new Request());
        }

        public RequestBuilder(ElasticsearchClient client) {
            this(client, GetCertificateInfoAction.INSTANCE);
        }
    }

}