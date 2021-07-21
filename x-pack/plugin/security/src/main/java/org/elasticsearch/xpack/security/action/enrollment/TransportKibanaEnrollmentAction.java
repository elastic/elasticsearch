/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.action.enrollment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentAction;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentRequest;
import org.elasticsearch.xpack.core.security.action.enrollment.KibanaEnrollmentResponse;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordAction;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequest;
import org.elasticsearch.xpack.core.security.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.ssl.KeyConfig;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.StoreKeyConfig;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;

import java.security.SecureRandom;
import java.security.cert.CertificateEncodingException;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;

public class TransportKibanaEnrollmentAction extends HandledTransportAction<KibanaEnrollmentRequest, KibanaEnrollmentResponse> {

    private static final Logger logger = LogManager.getLogger(TransportKibanaEnrollmentAction.class);

    private final Environment environment;
    private final Client client;
    private final SSLService sslService;

    @Inject public TransportKibanaEnrollmentAction(
        TransportService transportService,
        Client client,
        SSLService sslService,
        Environment environment,
        ActionFilters actionFilters) {
        super(KibanaEnrollmentAction.NAME, transportService, actionFilters, KibanaEnrollmentRequest::new);
        this.environment = environment;
        // Should we use a specific origin for this ? Are we satisfied with the auditability of the change password request as-is ?
        this.client = new OriginSettingClient(client, SECURITY_ORIGIN);
        this.sslService = sslService;
    }

    @Override protected void doExecute(Task task, KibanaEnrollmentRequest request, ActionListener<KibanaEnrollmentResponse> listener) {

        final KeyConfig keyConfig = sslService.getHttpTransportSSLConfiguration().keyConfig();
        if (keyConfig instanceof StoreKeyConfig == false) {
            listener.onFailure(new ElasticsearchException(
                "Unable to enroll kibana instance. Elasticsearch node HTTP layer SSL configuration is not configured with a keystore"));
            return;
        }
        List<X509Certificate> caCertificates;
        try {
            caCertificates = ((StoreKeyConfig) keyConfig).getPrivateKeyEntries(environment)
                .stream()
                .map(Tuple::v2)
                .filter(x509Certificate -> x509Certificate.getBasicConstraints() != -1)
                .collect(Collectors.toList());
        } catch (Exception e) {
            listener.onFailure(new ElasticsearchException("Unable to enroll kibana instance. Cannot retrieve CA certificate " +
                "for the HTTP layer of the Elasticsearch node.", e));
            return;
        }
        if (caCertificates.size() != 1) {
            listener.onFailure(new ElasticsearchException(
                "Unable to enroll kibana instance. Elasticsearch node HTTP layer SSL configuration Keystore " +
                "[xpack.security.http.ssl.keystore] doesn't contain a single PrivateKey entry where the associated " +
                "certificate is a CA certificate"));
        } else {
            String httpCa;
            try {
                httpCa = Base64.getUrlEncoder().encodeToString(caCertificates.get(0).getEncoded());
            } catch (CertificateEncodingException cee) {
                listener.onFailure(new ElasticsearchException(
                    "Unable to enroll kibana instance. Elasticsearch node HTTP layer SSL configuration uses a malformed CA certificate",
                    cee));
                return;
            }
            final char[] password = generateKibanaSystemPassword();
            final ChangePasswordRequest changePasswordRequest =
                new ChangePasswordRequestBuilder(client).username("kibana_system")
                    .password(password.clone(), Hasher.resolve(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(environment.settings())))
                    .request();
            client.execute(ChangePasswordAction.INSTANCE, changePasswordRequest, ActionListener.wrap(response -> {
                logger.debug("Successfully set the password for user [kibana_system] during kibana enrollment");
                listener.onResponse(new KibanaEnrollmentResponse(new SecureString(password), httpCa));
            }, e -> listener.onFailure(new ElasticsearchException("Failed to set the password for user [kibana_system]", e))));
        }

    }

    private char[] generateKibanaSystemPassword() {
        final char[] passwordChars = ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789~!@#$%^&*-_=+?").toCharArray();
        final SecureRandom secureRandom = new SecureRandom();
        int passwordLength = 14;
        char[] characters = new char[passwordLength];
        for (int i = 0; i < passwordLength; ++i) {
            characters[i] = passwordChars[secureRandom.nextInt(passwordChars.length)];
        }
        return characters;
    }

}
