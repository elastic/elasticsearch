/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.enrollment.BaseEnrollmentTokenGenerator;
import org.elasticsearch.xpack.security.enrollment.InternalEnrollmentTokenGenerator;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.xpack.core.XPackSettings.ENROLLMENT_ENABLED;
import static org.elasticsearch.xpack.security.authc.esnative.ReservedRealm.AUTOCONFIG_ELASTIC_PASSWORD_HASH;
import static org.elasticsearch.xpack.security.authc.esnative.ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD;
import static org.elasticsearch.xpack.security.tool.CommandUtils.generatePassword;

public class InitialNodeSecurityAutoConfiguration {

    private static final Logger LOGGER = LogManager.getLogger(InitialNodeSecurityAutoConfiguration.class);
    private static final BackoffPolicy BACKOFF_POLICY = BackoffPolicy.exponentialBackoff();

    private InitialNodeSecurityAutoConfiguration() {
        throw new IllegalStateException("Class should not be instantiated");
    }

    /**
     * Generates and displays a password for the elastic superuser, an enrollment token for kibana and an enrollment token for es
     * nodes, the first time a node starts as the first node in a cluster, when a terminal is attached.
     */
    public static void maybeGenerateEnrollmentTokensAndElasticCredentialsOnNodeStartup(
        NativeUsersStore nativeUsersStore,
        SecurityIndexManager securityIndexManager,
        SSLService sslService,
        Client client,
        Environment environment
    ) {
        // Assume the following auto-configuration must NOT run if enrollment is disabled when the node starts,
        // so no credentials or HTTPS CA fingerprint will be displayed in this case (in addition to no enrollment
        // tokens being generated).
        // This is not ideal because the {@code ENROLLMENT_ENABLED} setting is now interpreted as
        // "did the pre-startup configuration completed", in order to generate/display information assuming
        // and relying on that configuration being done.
        // TODO maybe we can improve the "did pre-start-up config run" check
        if (false == ENROLLMENT_ENABLED.get(environment.settings())) {
            return;
        }
        final InternalEnrollmentTokenGenerator enrollmentTokenGenerator = new InternalEnrollmentTokenGenerator(
            environment,
            sslService,
            client
        );

        final PrintStream out = getConsoleOutput();
        if (out == null) {
            LOGGER.info(
                "Auto-configuration will not generate a password for the elastic built-in superuser, as we cannot "
                    + " determine if there is a terminal attached to the elasticsearch process. You can use the"
                    + " `bin/elasticsearch-reset-password` tool to set the password for the elastic user."
            );
            return;
        }
        // if enrollment is enabled, we assume (and document this assumption) that the node is auto-configured in a specific way
        // wrt to TLS and cluster formation
        securityIndexManager.onStateRecovered(securityIndexState -> {
            if (false == securityIndexState.indexExists()) {
                // a starting node with {@code ENROLLMENT_ENABLED} set to true, and with no .security index,
                // must be the initial node of a cluster (starting for the first time and forming a cluster by itself)
                // Not always true, but in the cases where it's not (which involve deleting the .security index which
                // is now a system index), it's not a catastrophic position to be in either, because it only entails
                // that new tokens and possibly credentials are generated anew
                // TODO maybe we can improve the check that this is indeed the initial node
                String fingerprint;
                try {
                    fingerprint = enrollmentTokenGenerator.getHttpsCaFingerprint();
                    LOGGER.info(
                        "HTTPS has been configured with automatically generated certificates, "
                            + "and the CA's hex-encoded SHA-256 fingerprint is ["
                            + fingerprint
                            + "]"
                    );
                } catch (Exception e) {
                    fingerprint = null;
                    LOGGER.error("Failed to compute the HTTPS CA fingerprint, probably the certs are not auto-generated", e);
                }
                final String httpsCaFingerprint = fingerprint;
                GroupedActionListener<Map<String, String>> groupedActionListener = new GroupedActionListener<>(
                    ActionListener.wrap(results -> {
                        final Map<String, String> allResultsMap = new HashMap<>();
                        for (Map<String, String> result : results) {
                            allResultsMap.putAll(result);
                        }
                        final String elasticPassword = allResultsMap.get("generated_elastic_user_password");
                        final String kibanaEnrollmentToken = allResultsMap.get("kibana_enrollment_token");
                        final String nodeEnrollmentToken = allResultsMap.get("node_enrollment_token");
                        outputInformationToConsole(elasticPassword, kibanaEnrollmentToken, nodeEnrollmentToken, httpsCaFingerprint, out);
                    }, e -> { LOGGER.error("Unexpected exception during security auto-configuration", e); }),
                    3
                );
                // we only generate the elastic user password if the node has been auto-configured in a specific way, such that the first
                // time a node starts it will form a cluster by itself and can hold the .security index (which we assume it is when
                // {@code ENROLLMENT_ENABLED} is true), that the node process's output is a terminal and that the password is not
                // specified already via the two secure settings
                if (false == BOOTSTRAP_ELASTIC_PASSWORD.exists(environment.settings())
                    && false == AUTOCONFIG_ELASTIC_PASSWORD_HASH.exists(environment.settings())) {
                    final char[] elasticPassword = generatePassword(20);
                    nativeUsersStore.createElasticUser(elasticPassword, ActionListener.wrap(aVoid -> {
                        LOGGER.debug("elastic credentials generated successfully");
                        groupedActionListener.onResponse(Map.of("generated_elastic_user_password", new String(elasticPassword)));
                    }, e -> {
                        LOGGER.error("Failed to generate credentials for the elastic built-in superuser", e);
                        // null password in case of error
                        groupedActionListener.onResponse(Map.of());
                    }));
                } else {
                    if (false == BOOTSTRAP_ELASTIC_PASSWORD.exists(environment.settings())) {
                        LOGGER.info(
                            "Auto-configuration will not generate a password for the elastic built-in superuser, "
                                + "you should use the password specified in the node's secure setting ["
                                + BOOTSTRAP_ELASTIC_PASSWORD.getKey()
                                + "] in order to authenticate as elastic"
                        );
                    }
                    // empty password in case password generation is skipped
                    groupedActionListener.onResponse(Map.of("generated_elastic_user_password", ""));
                }
                final Iterator<TimeValue> backoff = BACKOFF_POLICY.iterator();
                enrollmentTokenGenerator.createKibanaEnrollmentToken(kibanaToken -> {
                    if (kibanaToken != null) {
                        try {
                            LOGGER.debug("Successfully generated the kibana enrollment token");
                            groupedActionListener.onResponse(Map.of("kibana_enrollment_token", kibanaToken.getEncoded()));
                        } catch (Exception e) {
                            LOGGER.error("Failed to encode kibana enrollment token", e);
                            groupedActionListener.onResponse(Map.of());
                        }
                    } else {
                        groupedActionListener.onResponse(Map.of());
                    }
                }, backoff);
                enrollmentTokenGenerator.maybeCreateNodeEnrollmentToken(encodedNodeToken -> {
                    if (encodedNodeToken != null) {
                        groupedActionListener.onResponse(Map.of("node_enrollment_token", encodedNodeToken));
                    } else {
                        groupedActionListener.onResponse(Map.of());
                    }
                }, backoff);
            }
        });
    }

    private static PrintStream getConsoleOutput() {
        final PrintStream output = BootstrapInfo.getConsoleOutput();
        if (output == null) {
            return null;
        }
        // Check if it has been closed, try to write something so that we trigger PrintStream#ensureOpen
        output.println();
        if (output.checkError()) {
            return null;
        }
        return output;
    }

    private static void outputInformationToConsole(
        String elasticPassword,
        String kibanaEnrollmentToken,
        String nodeEnrollmentToken,
        String caCertFingerprint,
        PrintStream out
    ) {
        StringBuilder builder = new StringBuilder();
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append("--------------------------------------------------------------------------------------------------------------");
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        if (elasticPassword == null) {
            builder.append("Unable to auto-generate the password for the elastic built-in superuser.");
        } else if (Strings.isEmpty(elasticPassword)) {
            builder.append("The generated password for the elastic built-in superuser has not been changed.");
        } else {
            builder.append("The generated password for the elastic built-in superuser is:");
            builder.append(System.lineSeparator());
            builder.append(elasticPassword);

        }
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        if (null != kibanaEnrollmentToken) {
            builder.append("The enrollment token for Kibana instances, valid for the next ");
            builder.append(BaseEnrollmentTokenGenerator.ENROLL_API_KEY_EXPIRATION_MINUTES);
            builder.append(" minutes:");
            builder.append(System.lineSeparator());
            builder.append(kibanaEnrollmentToken);
        } else {
            builder.append("Unable to generate an enrollment token for Kibana instances.");
        }
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        if (nodeEnrollmentToken == null) {
            builder.append("Unable to generate an enrollment token for Elasticsearch nodes.");
            builder.append(System.lineSeparator());
            builder.append(System.lineSeparator());
        } else if (false == Strings.isEmpty(nodeEnrollmentToken)) {
            builder.append("The enrollment token for Elasticsearch instances, valid for the next ");
            builder.append(BaseEnrollmentTokenGenerator.ENROLL_API_KEY_EXPIRATION_MINUTES);
            builder.append(" minutes:");
            builder.append(System.lineSeparator());
            builder.append(nodeEnrollmentToken);
            builder.append(System.lineSeparator());
            builder.append(System.lineSeparator());
        }
        if (null != caCertFingerprint) {
            builder.append("The hex-encoded SHA-256 fingerprint of the generated HTTPS CA DER-encoded certificate:");
            builder.append(System.lineSeparator());
            builder.append(caCertFingerprint);
            builder.append(System.lineSeparator());
        }
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append("You can complete the following actions at any time:");
        builder.append(System.lineSeparator());
        builder.append("Reset the password of the elastic built-in superuser with 'bin/elasticsearch-reset-password -u elastic'.");
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append("Generate an enrollment token for Kibana instances with 'bin/elasticsearch-create-enrollment-token -s kibana'.");
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append("Generate an enrollment token for Elasticsearch nodes with 'bin/elasticsearch-create-enrollment-token -s node'.");
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append("--------------------------------------------------------------------------------------------------------------");
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        out.println(builder);
    }
}
