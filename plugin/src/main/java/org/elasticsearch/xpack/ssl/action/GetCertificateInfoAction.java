/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl.action;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.ssl.SSLService;
import org.elasticsearch.xpack.ssl.cert.CertificateInfo;

/**
 * Action to obtain information about X.509 (SSL/TLS) certificates that are being used by X-Pack.
 * The primary use case is for tracking the expiry dates of certificates.
 */
public class GetCertificateInfoAction
        extends Action<GetCertificateInfoAction.Request, GetCertificateInfoAction.Response, GetCertificateInfoAction.RequestBuilder> {

    public static final GetCertificateInfoAction INSTANCE = new GetCertificateInfoAction();
    public static final String NAME = "cluster:monitor/xpack/ssl/certificates/get";

    private GetCertificateInfoAction() {
        super(NAME);
    }

    @Override
    public GetCertificateInfoAction.RequestBuilder newRequestBuilder(ElasticsearchClient client) {
        return new GetCertificateInfoAction.RequestBuilder(client, this);
    }

    @Override
    public GetCertificateInfoAction.Response newResponse() {
        return new GetCertificateInfoAction.Response();
    }

    public static class Request extends ActionRequest {

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

    }

    public static class Response extends ActionResponse implements ToXContentObject {

        private Collection<CertificateInfo> certificates;

        public Response() {
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
            super.writeTo(out);
            out.writeVInt(certificates.size());
            for (CertificateInfo cert : certificates) {
                cert.writeTo(out);
            }
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.certificates = new ArrayList<>();
            int count = in.readVInt();
            for (int i = 0; i < count; i++) {
                certificates.add(new CertificateInfo(in));
            }
        }
    }

    public static class RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder> {

        public RequestBuilder(ElasticsearchClient client, GetCertificateInfoAction action) {
            super(client, action, new Request());
        }

        public RequestBuilder(ElasticsearchClient client) {
            this(client, GetCertificateInfoAction.INSTANCE);
        }
    }

    public static class TransportAction extends HandledTransportAction<Request, Response> {

        private final SSLService sslService;

        @Inject
        public TransportAction(Settings settings, ThreadPool threadPool, TransportService transportService,
                               ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                               SSLService sslService) {
            super(settings, GetCertificateInfoAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver,
                    Request::new);
            this.sslService = sslService;
        }

        @Override
        protected void doExecute(Request request, ActionListener<Response> listener) {
            try {
                Collection<CertificateInfo> certificates = sslService.getLoadedCertificates();
                listener.onResponse(new Response(certificates));
            } catch (GeneralSecurityException | IOException e) {
                listener.onFailure(e);
            }
        }
    }
}