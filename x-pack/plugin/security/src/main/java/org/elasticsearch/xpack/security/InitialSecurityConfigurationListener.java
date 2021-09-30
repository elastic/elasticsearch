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
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.enrollment.InternalEnrollmentTokenGenerator;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.security.authc.esnative.ReservedRealm.AUTOCONFIG_BOOOTSTRAP_ELASTIC_PASSWORD_HASH;
import static org.elasticsearch.xpack.security.authc.esnative.ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD;
import static org.elasticsearch.xpack.security.tool.CommandUtils.generatePassword;

public class InitialSecurityConfigurationListener implements BiConsumer<SecurityIndexManager.State, SecurityIndexManager.State> {

    private static final String tokenKey = "token";
    private static final String fingerprintKey = "caCertFingerprint";
    private static final String passwordKey = "elasticPassword";

    private static final Logger LOGGER = LogManager.getLogger(InitialSecurityConfigurationListener.class);
    private final NativeUsersStore nativeUsersStore;
    private final SecurityIndexManager securityIndexManager;
    private final Environment environment;
    private final SSLService sslService;
    private final Client client;

    public InitialSecurityConfigurationListener(
        NativeUsersStore nativeUsersStore,
        SecurityIndexManager securityIndexManager,
        SSLService sslService,
        Client client,
        Environment environment
    ) {
        this.nativeUsersStore = nativeUsersStore;
        this.securityIndexManager = securityIndexManager;
        this.sslService = sslService;
        this.client = client;
        this.environment = environment;
    }

    @Override
    public void accept(SecurityIndexManager.State previousState, SecurityIndexManager.State currentState) {
        final PrintStream out = BootstrapInfo.getOriginalStandardOut();
        // Check if it has been closed, try to write something so that we trigger PrintStream#ensureOpen
        out.println();
        if (out.checkError()) {
            outputOnError(null);
            return;
        }
        if (previousState.equals(SecurityIndexManager.State.UNRECOVERED_STATE)
            && currentState.equals(SecurityIndexManager.State.UNRECOVERED_STATE) == false
            && securityIndexManager.indexExists() == false
            && XPackSettings.ENROLLMENT_ENABLED.get(environment.settings())) {
            GroupedActionListener<Map<String, String>> groupedActionListener = new GroupedActionListener<>(ActionListener.wrap(results -> {
                final Map<String, String> allResultsMap = new HashMap<>();
                for (Map<String, String> result : results) {
                    allResultsMap.putAll(result);
                }
                final String password = allResultsMap.get(passwordKey);
                final String token = allResultsMap.get(tokenKey);
                final String caCertFingerprint = allResultsMap.get(fingerprintKey);
                outputInformationToConsole(password, token, caCertFingerprint, out);
            }, this::outputOnError), 2);

            if (false == BOOTSTRAP_ELASTIC_PASSWORD.exists(environment.settings())
                && false == AUTOCONFIG_BOOOTSTRAP_ELASTIC_PASSWORD_HASH.exists(environment.settings())) {
                final SecureString elasticPassword = new SecureString(generatePassword(20));
                nativeUsersStore.updateReservedUser(
                    ElasticUser.NAME,
                    elasticPassword.getChars(),
                    DocWriteRequest.OpType.CREATE,
                    WriteRequest.RefreshPolicy.IMMEDIATE,
                    groupedActionListener.map(ignore -> Map.of(passwordKey, elasticPassword.toString()))
                );
            } else {
                groupedActionListener.onResponse(Map.of());
            }
            final InternalEnrollmentTokenGenerator enrollmentTokenGenerator = new InternalEnrollmentTokenGenerator(
                environment,
                sslService,
                client
            );
            enrollmentTokenGenerator.createKibanaEnrollmentToken(
                groupedActionListener.map(token -> token == null ? Map.of() : Map.of(tokenKey, token.getEncoded(), fingerprintKey,
                    token.getFingerprint()))
            );
            securityIndexManager.removeStateListener(this);
        }
    }

    private void outputInformationToConsole(String elasticPassword, String enrollmentToken, String caCertFingerprint, PrintStream out) {
        StringBuilder builder = new StringBuilder();
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append("-----------------------------------------------------------------");
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        if (null != elasticPassword) {
            builder.append("Password for the elastic user is: ");
            builder.append(elasticPassword);
        } else {
            builder.append("Unable to set the password for the elastic user automatically");
        }
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        if (null != enrollmentToken) {
            builder.append("Enrollment token for kibana, valid for the next 30 minutes:");
            builder.append(System.lineSeparator());
            builder.append(enrollmentToken);
            builder.append(System.lineSeparator());
        } else {
            builder.append("Unable to generate an enrollment token for kibana automatically");
            builder.append(System.lineSeparator());
        }
        builder.append(System.lineSeparator());
        if (null != caCertFingerprint) {
            builder.append("Fingerprint of the generated CA certificate for HTTP:");
            builder.append(System.lineSeparator());
            builder.append(caCertFingerprint);
            builder.append(System.lineSeparator());
        }
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append("You can use 'bin/elasticsearch-reset-elastic-password' at any time");
        builder.append(System.lineSeparator());
        builder.append("in order to set or reset the password for the elastic user.");
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append("You can use 'bin/elasticsearch-create-enrollment-token -s kibana' at any time");
        builder.append(System.lineSeparator());
        builder.append("in order to get a new, valid, enrollment token for kibana.");
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append("You can use 'bin/elasticsearch-create-enrollment-token -s node' at any time");
        builder.append(System.lineSeparator());
        builder.append("in order to get a new, valid, enrollment token for new elasticsearch nodes.");
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        builder.append("-----------------------------------------------------------------");
        builder.append(System.lineSeparator());
        builder.append(System.lineSeparator());
        out.println(builder);
    }

    private void outputOnError(@Nullable Exception e) {
        if (e instanceof VersionConflictEngineException == false) {
            LOGGER.info("");
            LOGGER.info("-----------------------------------------------------------------");
            LOGGER.info("");
            LOGGER.info("Unable set the password for the elastic and generate a kibana ");
            LOGGER.info("enrollment token automatically.");
            LOGGER.info("");
            LOGGER.info("You can use 'bin/elasticsearch-reset-elastic-password'");
            LOGGER.info("in order to set the password for the elastic user.");
            LOGGER.info("");
            LOGGER.info("You can use 'bin/elasticsearch-create-enrollment-token -s kibana'");
            LOGGER.info("in order to generate an enrollment token for kibana.");
            LOGGER.info("");
            LOGGER.info("You can use 'bin/elasticsearch-create-enrollment-token -s node'");
            LOGGER.info("in order to generate an enrollment token for new elasticsearch nodes.");
            LOGGER.info("");
            LOGGER.info("-----------------------------------------------------------------");
            LOGGER.info("");
        }
        if (null != e) {
            LOGGER.warn("Error setting initial password for elastic and generating a kibana enrollment token", e);
        }
    }
}
