/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.bootstrap.ConsoleLoader;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.enrollment.InternalEnrollmentTokenGenerator;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.nio.charset.StandardCharsets;
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
        Environment environment,
        OnNodeStartedListener onNodeStartedListener,
        ThreadPool threadPool
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

        final ConsoleLoader.Console console = getConsole();
        if (console == null) {
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
                // a lot of stuff runs when a node just started, and the autoconfiguration is not time-critical
                // and nothing else depends on it; be a good sport and wait a couple
                onNodeStartedListener.run(() -> threadPool.schedule(new AbstractRunnable() {

                    @Override
                    public void onFailure(Exception e) {
                        LOGGER.error("Unexpected exception when auto configuring the initial node for Security", e);
                    }

                    @Override
                    protected void doRun() {
                        // the HTTP address is guaranteed to be bound only after the node started
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
                            3,
                            ActionListener.wrap(results -> {
                                final Map<String, String> allResultsMap = new HashMap<>();
                                for (Map<String, String> result : results) {
                                    allResultsMap.putAll(result);
                                }
                                final String elasticPassword = allResultsMap.get("generated_elastic_user_password");
                                final String kibanaEnrollmentToken = allResultsMap.get("kibana_enrollment_token");
                                final String nodeEnrollmentToken = allResultsMap.get("node_enrollment_token");
                                outputInformationToConsole(
                                    elasticPassword,
                                    kibanaEnrollmentToken,
                                    nodeEnrollmentToken,
                                    httpsCaFingerprint,
                                    console
                                );
                            }, e -> LOGGER.error("Unexpected exception during security auto-configuration", e))
                        );
                        // we only generate the elastic user password if the node has been auto-configured in a specific way, such that the
                        // first time a node starts it will form a cluster by itself and can hold the .security index (which we assume
                        // it is when {@code ENROLLMENT_ENABLED} is true), that the node process's output is a terminal and that the
                        // password is not specified already via the two secure settings
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
                }, TimeValue.timeValueSeconds(9), threadPool.generic()));
            }
        });
    }

    private static ConsoleLoader.Console getConsole() {
        final ConsoleLoader.Console console = BootstrapInfo.getConsole();
        if (console == null) {
            return null;
        }
        // Check if it has been closed, try to write something so that we trigger PrintStream#ensureOpen
        console.printStream().println();
        if (console.printStream().checkError()) {
            return null;
        }
        return console;
    }

    private static void outputInformationToConsole(
        String elasticPassword,
        String kibanaEnrollmentToken,
        String nodeEnrollmentToken,
        String caCertFingerprint,
        ConsoleLoader.Console console
    ) {
        // Use eye-catching pictograms to output the configuration information, but only if the
        // console charset utilizes some known variation of UTF, otherwise we risk that the encoder
        // cannot handle the special unicode code points and will display funky question marks instead
        boolean useUnicode = StandardCharsets.UTF_8.equals(console.charset())
            || StandardCharsets.UTF_16.equals(console.charset())
            || StandardCharsets.UTF_16LE.equals(console.charset())
            || StandardCharsets.UTF_16BE.equals(console.charset());
        final String infoBullet = useUnicode ? "\u2139\uFE0F" : "->";
        final String bullet = useUnicode ? "\u2022" : "*";
        final String hyphenBullet = useUnicode ? "\u2043" : "-";
        final String errorBullet = useUnicode ? "\u274C" : "X";
        final String successBullet = useUnicode ? "\u2705" : "->";
        final String horizontalBorderLine = useUnicode ? "\u2501" : "-";
        final String boldOnANSI = console.ansiEnabled() ? "\u001B[1m" : "";
        final String boldOffANSI = console.ansiEnabled() ? "\u001B[22m" : "";
        final String cmdOn = "`";
        final String cmdOff = "`";
        final int horizontalBorderLength = console.width().get();
        StringBuilder builder = new StringBuilder();
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append(horizontalBorderLine.repeat(horizontalBorderLength));
        builder.append(System.lineSeparator());
        builder.append(successBullet + " Elasticsearch security features have been automatically configured!");
        builder.append(System.lineSeparator());
        builder.append(successBullet + " Authentication is enabled and cluster connections are encrypted.");
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        if (elasticPassword == null) {
            builder.append(
                errorBullet
                    + " Unable to auto-generate the password for the "
                    + boldOnANSI
                    + "elastic"
                    + boldOffANSI
                    + " built-in superuser."
            );
        } else if (false == Strings.isEmpty(elasticPassword)) {
            builder.append(
                infoBullet
                    + "  Password for the "
                    + boldOnANSI
                    + "elastic"
                    + boldOffANSI
                    + " user (reset with "
                    + cmdOn
                    + "bin/elasticsearch-reset-password -u elastic"
                    + cmdOff
                    + "):"
            );
            builder.append(System.lineSeparator());
            builder.append("  " + boldOnANSI + elasticPassword + boldOffANSI);
        }
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());

        if (null != caCertFingerprint) {
            builder.append(infoBullet + "  HTTP CA certificate SHA-256 fingerprint:");
            builder.append(System.lineSeparator());
            builder.append("  " + boldOnANSI + caCertFingerprint + boldOffANSI);
        }
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());

        if (null != kibanaEnrollmentToken) {
            builder.append(infoBullet + "  Configure Kibana to use this cluster:");
            builder.append(System.lineSeparator());
            builder.append(bullet + " Run Kibana and click the configuration link in the terminal when Kibana starts.");
            builder.append(System.lineSeparator());
            builder.append(bullet + " Copy the following enrollment token and paste it into Kibana in your browser ");
            builder.append("(valid for the next 30 minutes):");
            builder.append(System.lineSeparator());
            builder.append("  " + boldOnANSI + kibanaEnrollmentToken + boldOffANSI);
        } else {
            builder.append(errorBullet + " Unable to generate an enrollment token for Kibana instances, ");
            builder.append("try invoking " + cmdOn + "bin/elasticsearch-create-enrollment-token -s kibana" + cmdOff + ".");
        }
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());

        // Node enrollment token
        if (null == nodeEnrollmentToken) {
            builder.append(errorBullet + " An enrollment token to enroll new nodes wasn't generated.");
            builder.append(" To add nodes and enroll them into this cluster:");
            builder.append(System.lineSeparator());
            builder.append(bullet + " On this node:");
            builder.append(System.lineSeparator());
            builder.append(
                "  "
                    + hyphenBullet
                    + " Create an enrollment token with "
                    + cmdOn
                    + "bin/elasticsearch-create-enrollment-token -s node"
                    + cmdOff
                    + "."
            );
            builder.append(System.lineSeparator());
            builder.append("  " + hyphenBullet + " Restart Elasticsearch.");
            builder.append(System.lineSeparator());
            builder.append(bullet + " On other nodes:");
            builder.append(System.lineSeparator());
            builder.append(
                "  "
                    + hyphenBullet
                    + " Start Elasticsearch with "
                    + cmdOn
                    + "bin/elasticsearch --enrollment-token <token>"
                    + cmdOff
                    + ", using the enrollment token that you generated."
            );
        } else if (Strings.isEmpty(nodeEnrollmentToken)) {
            builder.append(infoBullet + "  Configure other nodes to join this cluster:");
            builder.append(System.lineSeparator());
            builder.append(bullet + " On this node:");
            builder.append(System.lineSeparator());
            builder.append(
                "  "
                    + hyphenBullet
                    + " Create an enrollment token with "
                    + cmdOn
                    + "bin/elasticsearch-create-enrollment-token -s node"
                    + cmdOff
                    + "."
            );
            builder.append(System.lineSeparator());
            builder.append(
                "  "
                    + hyphenBullet
                    + " Uncomment the "
                    + boldOnANSI
                    + "transport.host"
                    + boldOffANSI
                    + " setting at the end of "
                    + boldOnANSI
                    + "config/elasticsearch.yml"
                    + boldOffANSI
                    + "."
            );
            builder.append(System.lineSeparator());
            builder.append("  " + hyphenBullet + " Restart Elasticsearch.");
            builder.append(System.lineSeparator());
            builder.append(bullet + " On other nodes:");
            builder.append(System.lineSeparator());
            builder.append(
                "  "
                    + hyphenBullet
                    + " Start Elasticsearch with "
                    + cmdOn
                    + "bin/elasticsearch --enrollment-token <token>"
                    + cmdOff
                    + ", using the enrollment token that you generated."
            );
        } else {
            builder.append(infoBullet + " Configure other nodes to join this cluster:");
            builder.append(System.lineSeparator());
            builder.append(
                bullet
                    + " Copy the following enrollment token and start new Elasticsearch nodes with "
                    + cmdOn
                    + "bin/elasticsearch --enrollment-token <token>"
                    + cmdOff
                    + " (valid for the next 30 minutes):"
            );
            builder.append(System.lineSeparator());
            builder.append("  " + boldOnANSI + nodeEnrollmentToken + boldOffANSI);
            builder.append(System.lineSeparator());
            builder.append(System.lineSeparator());
            builder.append("  If you're running in Docker, copy the enrollment token and run:");
            builder.append(System.lineSeparator());
            builder.append(
                "  "
                    + cmdOn
                    + "docker run -e \"ENROLLMENT_TOKEN=<token>\" docker.elastic.co/elasticsearch/elasticsearch:"
                    + Version.CURRENT
                    + cmdOff
            );
        }

        builder.append(System.lineSeparator());
        builder.append(horizontalBorderLine.repeat(horizontalBorderLength));
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());

        console.printStream().println(builder);
    }

    interface OnNodeStartedListener {
        void run(Runnable runnable);
    }
}
