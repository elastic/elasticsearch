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
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.enrollment.BaseEnrollmentTokenGenerator;
import org.elasticsearch.xpack.security.enrollment.InternalEnrollmentTokenGenerator;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiPrintStream;
import org.fusesource.jansi.AnsiType;

import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.core.XPackSettings.ENROLLMENT_ENABLED;
import static org.elasticsearch.xpack.security.authc.esnative.ReservedRealm.AUTOCONFIG_ELASTIC_PASSWORD_HASH;
import static org.elasticsearch.xpack.security.authc.esnative.ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD;
import static org.elasticsearch.xpack.security.tool.CommandUtils.generatePassword;
import static org.fusesource.jansi.Ansi.ansi;

public class InitialNodeSecurityAutoConfiguration {

    private static final Logger LOGGER = LogManager.getLogger(InitialNodeSecurityAutoConfiguration.class);

    /**
     * Generates and display
     * @param nativeUsersStore
     * @param securityIndexManager
     * @param sslService
     * @param client
     * @param environment
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
        // if enrollment is enabled, we assume (and document this assumption) that the node is auto-configured in a specific way
        // wrt to TLS and cluster formation
        final AnsiPrintStream out = BootstrapInfo.getTerminalPrintStream();
        final boolean processOutputAttachedToTerminal = out != null &&
            out.getType() != AnsiType.Redirected && // output is a pipe
            out.getType() != AnsiType.Unsupported && // could not determine terminal type
            out.getTerminalWidth() > 0; // hack to determine when logs are output to a terminal inside a docker container, but the docker
        securityIndexManager.onStateRecovered(securityIndexState -> {
            if (false == securityIndexState.indexExists()) {
                // a starting node with {@code ENROLLMENT_ENABLED} set to true, and with no .security index,
                // must be the initial node of a cluster (starting for the first time and forming a cluster by itself)
                // Not always true, but in the cases where it's not (which involve deleting the .security index which
                // is now a system index), it's not a catastrophic position to be in either, because it only entails
                // that new tokens and possibly credentials are generated anew
                // TODO maybe we can improve the check that this is indeed the initial node
                assert out != null;
                String fingerprint;
                try {
                    fingerprint = enrollmentTokenGenerator.getHttpsCaFingerprint();
                    LOGGER.info("HTTPS has been configured with automatically generated certificates, " +
                        "and the CA's hex-encoded SHA-256 fingerprint is [" + fingerprint + "]");
                } catch (Exception e) {
                    fingerprint = null;
                    LOGGER.error("Failed to compute the HTTPS CA fingerprint, probably the certs are not auto-generated", e);
                }
                // output itself is redirected;
                if (false == processOutputAttachedToTerminal) {
                    // Avoid outputting secrets if output is not a terminal, because we assume that the output could eventually end up in a file and
                    // we should avoid printing credentials (even temporary ones) to files.
                    // The HTTPS CA fingerprint, which is not a secret, IS printed, but as a LOG ENTRY rather than a saliently formatted
                    // human-friendly message, in order to avoid breaking parsers that expect the node output to only contain log entries.
                    return;
                }
                final String httpsCaFingerprint = fingerprint;
                GroupedActionListener<Map<String, String>> groupedActionListener =
                    new GroupedActionListener<>(ActionListener.wrap(results -> {
                        final Map<String, String> allResultsMap = new HashMap<>();
                        for (Map<String, String> result : results) {
                            allResultsMap.putAll(result);
                        }
                        final String elasticPassword = allResultsMap.get("generated_elastic_user_password");
                        final String kibanaEnrollmentToken = allResultsMap.get("kibana_enrollment_token");
                        final String nodeEnrollmentToken = allResultsMap.get("node_enrollment_token");
                        outputInformationToConsole(elasticPassword, kibanaEnrollmentToken, nodeEnrollmentToken, httpsCaFingerprint, out);
                    }, e -> {
                        LOGGER.error("Unexpected exception during security auto-configuration", e);
                    }), 3);
                // we only generate the elastic user password if the node has been auto-configured in a specific way, such that the first
                // time a node starts it will form a cluster by itself and can hold the .security index (which we assume it is when
                // {@code ENROLLMENT_ENABLED} is true), that the node process's output is a terminal and that the password is not
                // specified already via the two secure settings
                if (false == BOOTSTRAP_ELASTIC_PASSWORD.exists(environment.settings()) &&
                    false == AUTOCONFIG_ELASTIC_PASSWORD_HASH.exists(environment.settings())) {
                    final char[] elasticPassword = generatePassword(20);
                    nativeUsersStore.createElasticUser(elasticPassword, ActionListener.wrap(aVoid -> {
                        LOGGER.debug("elastic credentials generated successfully");
                        groupedActionListener.onResponse(Map.of(
                            "generated_elastic_user_password", new String(elasticPassword)));
                    }, e -> {
                        LOGGER.error("Failed to generate credentials for the elastic built-in superuser", e);
                        // null password in case of error
                        groupedActionListener.onResponse(Map.of());
                    }));
                } else {
                    if (false == BOOTSTRAP_ELASTIC_PASSWORD.exists(environment.settings())) {
                        LOGGER.info("Auto-configuration will not generate a password for the elastic built-in superuser, " +
                            "you should use the password specified in the node's secure setting [" + BOOTSTRAP_ELASTIC_PASSWORD.getKey() +
                            "] in order to authenticate as elastic");
                    } else {
                        LOGGER.debug("The password for the elastic built-in superuser has already been generated, the [" +
                            AUTOCONFIG_ELASTIC_PASSWORD_HASH.getKey() + "] secure setting is set");
                    }
                    // empty password in case password generation is skyped
                    groupedActionListener.onResponse(Map.of("generated_elastic_user_password", ""));
                }
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
                });
                enrollmentTokenGenerator.maybeCreateNodeEnrollmentToken(encodedNodeToken -> {
                    if (encodedNodeToken != null) {
                        groupedActionListener.onResponse(Map.of("node_enrollment_token", encodedNodeToken));
                    } else {
                        groupedActionListener.onResponse(Map.of());
                    }
                });
            }
        });
    }

    private static void outputInformationToConsole(String elasticPassword, String kibanaEnrollmentToken,
                                                   String nodeEnrollmentToken, String caCertFingerprint,
                                                   AnsiPrintStream out) {
        final Ansi ansi = ansi();
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        ansi.a("-".repeat(Math.max(1, out.getTerminalWidth())));
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        ansi.bold();
        if (elasticPassword == null) {
            ansi.a("Unable to auto-generate the password for the ").a(Ansi.Attribute.ITALIC).a("elastic").a(Ansi.Attribute.ITALIC_OFF)
                .a(" built-in superuser.");
        } else if (Strings.isEmpty(elasticPassword)) {
            ansi.a("The generated password for the ").a(Ansi.Attribute.ITALIC).a("elastic").a(Ansi.Attribute.ITALIC_OFF)
                .a(" built-in superuser has not been changed.");
        } else {
            ansi.a("The generated password for the ").a(Ansi.Attribute.ITALIC).a("elastic").a(Ansi.Attribute.ITALIC_OFF)
                .a(" built-in superuser is:");
            ansi.a(System.lineSeparator());
            ansi.a(Ansi.Attribute.UNDERLINE);
            ansi.a(elasticPassword);
            ansi.a(Ansi.Attribute.UNDERLINE_OFF);
        }
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        if (null != kibanaEnrollmentToken) {
            ansi.a("The enrollment token for ").a(Ansi.Attribute.ITALIC).a("Kibana").a(Ansi.Attribute.ITALIC_OFF)
                .a(" instances, valid for the next ").a(BaseEnrollmentTokenGenerator.ENROLL_API_KEY_EXPIRATION_MINUTES)
                .a(" minutes:");
            ansi.a(System.lineSeparator());
            ansi.a(Ansi.Attribute.UNDERLINE);
            ansi.a(kibanaEnrollmentToken);
            ansi.a(Ansi.Attribute.UNDERLINE_OFF);
        } else {
            ansi.a("Unable to generate an enrollment token for Kibana instances.");
        }
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        if (nodeEnrollmentToken == null) {
            ansi.a("Unable to generate an enrollment token for Elasticsearch nodes.");
            ansi.a(System.lineSeparator());
            ansi.a(System.lineSeparator());
        } else if (false == Strings.isEmpty(nodeEnrollmentToken)) {
            ansi.a("The enrollment token for ").a(Ansi.Attribute.ITALIC).a("Elasticsearch").a(Ansi.Attribute.ITALIC_OFF)
                .a(" instances, valid for the next ").a(BaseEnrollmentTokenGenerator.ENROLL_API_KEY_EXPIRATION_MINUTES)
                .a(" minutes:");
            ansi.a(System.lineSeparator());
            ansi.a(Ansi.Attribute.UNDERLINE);
            ansi.a(nodeEnrollmentToken);
            ansi.a(Ansi.Attribute.UNDERLINE_OFF);
            ansi.a(System.lineSeparator());
            ansi.a(System.lineSeparator());
        }
        if (null != caCertFingerprint) {
            ansi.a("The hex-encoded SHA-256 fingerprint of the generated HTTPS CA DER-encoded certificate:");
            ansi.a(System.lineSeparator());
            ansi.a(Ansi.Attribute.UNDERLINE);
            ansi.a(caCertFingerprint);
            ansi.a(Ansi.Attribute.UNDERLINE_OFF);
            ansi.a(System.lineSeparator());
        }
        ansi.boldOff();
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        ansi.a("You can use 'bin/elasticsearch-reset-elastic-password' at any time in order to set or reset the password for " +
                "the elastic user.");
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        ansi.a("You can use 'bin/elasticsearch-create-enrollment-token -s kibana' at any time in order to get a new enrollment token for " +
            "kibana instances.");
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        ansi.a("You can use 'bin/elasticsearch-create-enrollment-token -s node' at any time in order to get a new enrollment token for " +
                "elasticsearch nodes.");
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        ansi.a("-".repeat(Math.max(1, out.getTerminalWidth())));
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        out.println(ansi);
    }
}
