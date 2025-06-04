/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.enrollment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.ssl.StoreKeyConfig;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentResponse;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.ssl.SSLService;

import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;

public class TransportKibanaEnrollmentAction extends HandledTransportAction<KibanaEnrollmentRequest, KibanaEnrollmentResponse> {

    private static final Logger logger = LogManager.getLogger(TransportKibanaEnrollmentAction.class);

    private final Client client;
    private final SSLService sslService;

    @Inject
    public TransportKibanaEnrollmentAction(
        TransportService transportService,
        Client client,
        SSLService sslService,
        ActionFilters actionFilters
    ) {
        super(
            KibanaEnrollmentAction.NAME,
            transportService,
            actionFilters,
            KibanaEnrollmentRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.client = new OriginSettingClient(client, SECURITY_ORIGIN);
        this.sslService = sslService;
    }

    @Override
    protected void doExecute(Task task, KibanaEnrollmentRequest request, ActionListener<KibanaEnrollmentResponse> listener) {

        final SslKeyConfig keyConfig = sslService.getHttpTransportSSLConfiguration().keyConfig();
        if (keyConfig instanceof StoreKeyConfig == false) {
            listener.onFailure(
                new ElasticsearchException(
                    "Unable to enroll kibana instance. Elasticsearch node HTTP layer SSL configuration is not configured with a keystore"
                )
            );
            return;
        }
        List<X509Certificate> caCertificates;
        try {
            caCertificates = ((StoreKeyConfig) keyConfig).getKeys()
                .stream()
                .map(Tuple::v2)
                .filter(x509Certificate -> x509Certificate.getBasicConstraints() != -1)
                .collect(Collectors.toList());
        } catch (Exception e) {
            listener.onFailure(
                new ElasticsearchException(
                    "Unable to enroll kibana instance. Cannot retrieve CA certificate " + "for the HTTP layer of the Elasticsearch node.",
                    e
                )
            );
            return;
        }
        if (caCertificates.size() != 1) {
            listener.onFailure(
                new ElasticsearchException(
                    "Unable to enroll kibana instance. Elasticsearch node HTTP layer SSL configuration Keystore "
                        + "[xpack.security.http.ssl.keystore] doesn't contain a single PrivateKey entry where the associated "
                        + "certificate is a CA certificate"
                )
            );
        } else {
            String httpCa;
            try {
                httpCa = Base64.getEncoder().encodeToString(caCertificates.get(0).getEncoded());
            } catch (CertificateEncodingException cee) {
                listener.onFailure(
                    new ElasticsearchException(
                        "Unable to enroll kibana instance. Elasticsearch node HTTP layer SSL configuration uses a malformed CA certificate",
                        cee
                    )
                );
                return;
            }
            final CreateServiceAccountTokenRequest createServiceAccountTokenRequest = new CreateServiceAccountTokenRequest(
                "elastic",
                "kibana",
                getTokenName()
            );
            client.execute(CreateServiceAccountTokenAction.INSTANCE, createServiceAccountTokenRequest, ActionListener.wrap(response -> {
                logger.debug(
                    "Successfully created token [{}] for the [elastic/kibana] service account during kibana enrollment",
                    response.getName()
                );
                listener.onResponse(new KibanaEnrollmentResponse(response.getName(), response.getValue(), httpCa));
            }, e -> listener.onFailure(new ElasticsearchException("Failed to create token for the [elastic/kibana] service account", e))));
        }
    }

    protected static String getTokenName() {
        final ZonedDateTime enrollTime = ZonedDateTime.now(ZoneOffset.UTC);
        final String prefix = "enroll-process-token-";
        return prefix + enrollTime.toInstant().toEpochMilli();
    }
}
