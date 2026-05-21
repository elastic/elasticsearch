/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.workload;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.env.Environment;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;

public class TransportRunWorkloadIssuerTestAction extends HandledTransportAction<WorkloadIssuerTestAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportRunWorkloadIssuerTestAction.class);

    private static final String ISSUER_URL = "http://workload-identity-issuer-internal.eu-west-1.aws.svc.qa.elastic.cloud/token";
    private static final String CERT_SETTING = "xpack.inference.elastic.http.ssl.certificate";
    private static final String KEY_SETTING = "xpack.inference.elastic.http.ssl.key";

    private final Settings settings;
    private final Environment environment;

    @Inject
    public TransportRunWorkloadIssuerTestAction(
        TransportService transportService,
        ActionFilters actionFilters,
        Settings settings,
        Environment environment
    ) {
        super(
            WorkloadIssuerTestAction.NAME,
            transportService,
            actionFilters,
            WorkloadIssuerTestAction.Request::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.settings = settings;
        this.environment = environment;
    }

    @Override
    protected void doExecute(Task task, WorkloadIssuerTestAction.Request request, ActionListener<AcknowledgedResponse> listener) {
        final HttpClient httpClient;
        final HttpRequest httpRequest;
        try {
            final SSLContext sslContext = buildSslContext();
            httpClient = HttpClient.newBuilder().sslContext(sslContext).build();
            httpRequest = HttpRequest.newBuilder(URI.create(ISSUER_URL))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("""
                    {
                        "aud":"test_audience",
                        "region":"eu-west-1"
                    }
                    """))
                .build();
        } catch (Exception e) {
            logger.error(() -> "workload issuer test: failed to prepare mTLS request", e);
            listener.onFailure(e);
            return;
        }

        httpClient.sendAsync(httpRequest, HttpResponse.BodyHandlers.ofString()).whenComplete((response, error) -> {
            if (error != null) {
                logger.error(() -> "workload issuer test: request failed", error);
                listener.onFailure(error instanceof Exception ex ? ex : new RuntimeException(error));
                return;
            }
            logger.info("workload issuer test response: status={} body={}", response.statusCode(), response.body());
            listener.onResponse(AcknowledgedResponse.TRUE);
        });
    }

    private SSLContext buildSslContext() throws Exception {
        final String certSettingValue = settings.get(CERT_SETTING);
        final String keySettingValue = settings.get(KEY_SETTING);
        if (certSettingValue == null || keySettingValue == null) {
            throw new IllegalStateException(
                "workload issuer test requires [" + CERT_SETTING + "] and [" + KEY_SETTING + "] to be configured"
            );
        }
        final Path certPath = environment.configDir().resolve(certSettingValue);
        final Path keyPath = environment.configDir().resolve(keySettingValue);

        final PrivateKey privateKey = PemUtils.readPrivateKey(keyPath, () -> new char[0]);
        final List<Certificate> certificates = PemUtils.readCertificates(List.of(certPath));
        final KeyManager keyManager = KeyStoreUtil.createKeyManager(certificates.toArray(Certificate[]::new), privateKey, new char[0]);

        final SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(new KeyManager[] { keyManager }, null, null);
        return sslContext;
    }
}
