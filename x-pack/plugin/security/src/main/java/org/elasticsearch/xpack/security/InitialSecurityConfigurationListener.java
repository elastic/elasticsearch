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
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiPrintStream;
import org.fusesource.jansi.AnsiType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.security.authc.esnative.ReservedRealm.AUTOCONFIG_ELASTIC_PASSWORD_HASH;
import static org.elasticsearch.xpack.security.authc.esnative.ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD;
import static org.elasticsearch.xpack.security.tool.CommandUtils.generatePassword;
import static org.fusesource.jansi.Ansi.ansi;

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
        final AnsiPrintStream out = BootstrapInfo.getTerminalPrintStream();
        if (out == null) {
            return;
        }
        AnsiType ansiType = out.getType();
        if (ansiType == AnsiType.Redirected || // output is a pipe
            ansiType == AnsiType.Unsupported || // could not determine terminal type
            out.getTerminalWidth() <= 0 // hack when logs are output to a terminal inside a docker container, but the docker output
            // itself is redirected
        ) {
            // TODO log
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
                && false == AUTOCONFIG_ELASTIC_PASSWORD_HASH.exists(environment.settings())) {
                final SecureString elasticPassword = new SecureString(generatePassword(20));
                nativeUsersStore.createElasticUser(elasticPassword.getChars(), groupedActionListener.map(ignore -> Map.of(passwordKey,
                    elasticPassword.toString())));
            } else {
                groupedActionListener.onResponse(Map.of());
            }
            final InternalEnrollmentTokenGenerator enrollmentTokenGenerator = new InternalEnrollmentTokenGenerator(
                environment,
                sslService,
                client
            );
            // TODO also generate and show the node enrollment token
            enrollmentTokenGenerator.createKibanaEnrollmentToken(
                groupedActionListener.map(token -> token == null ? Map.of() : Map.of(tokenKey, token.getEncoded(), fingerprintKey,
                    token.getFingerprint()))
            );
            securityIndexManager.removeStateListener(this);
        }
    }

    private void outputInformationToConsole(String elasticPassword, String enrollmentToken, String caCertFingerprint, AnsiPrintStream out) {
        Ansi ansi = ansi();
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        ansi.a("-".repeat(Math.max(1, out.getTerminalWidth())));
        ansi.bold();
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        if (null != elasticPassword) {
            ansi.a("Password for the ").a(Ansi.Attribute.ITALIC).a("elastic").a(Ansi.Attribute.ITALIC_OFF).a(" built-in superuser:");
            ansi.a(System.lineSeparator());
            ansi.a(Ansi.Attribute.UNDERLINE);
            ansi.a(elasticPassword);
            ansi.a(Ansi.Attribute.UNDERLINE_OFF);
        } else {
            ansi.a("Unable to set the password for the elastic user automatically");
        }
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        if (null != enrollmentToken) {
            ansi.a("Enrollment token for ").a(Ansi.Attribute.ITALIC).a("Kibana").a(Ansi.Attribute.ITALIC_OFF).a(", valid for the next 30 minutes:");
            ansi.a(System.lineSeparator());
            ansi.a(Ansi.Attribute.UNDERLINE);
            ansi.a(enrollmentToken);
            ansi.a(Ansi.Attribute.UNDERLINE_OFF);
            ansi.a(System.lineSeparator());
        } else {
            ansi.a("Unable to generate an enrollment token for Kibana automatically");
            ansi.a(System.lineSeparator());
        }
        // TODO generate node enrollment tokens as well
        ansi.a(System.lineSeparator());
        if (null != caCertFingerprint) {
            ansi.a("Hex-encoded SHA-256 fingerprint of the generated HTTPS CA DER-encoded certificate:");
            ansi.a(System.lineSeparator());
            ansi.a(Ansi.Attribute.UNDERLINE);
            ansi.a(caCertFingerprint);
            ansi.a(Ansi.Attribute.UNDERLINE_OFF);
            ansi.a(System.lineSeparator());
        }
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        ansi.a("You can use 'bin/elasticsearch-reset-elastic-password' at any time");
        ansi.a(System.lineSeparator());
        ansi.a("in order to set or reset the password for the elastic user.");
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        ansi.a("You can use 'bin/elasticsearch-create-enrollment-token -s kibana' at any time");
        ansi.a(System.lineSeparator());
        ansi.a("in order to get a new, valid, enrollment token for kibana.");
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        ansi.a("You can use 'bin/elasticsearch-create-enrollment-token -s node' at any time");
        ansi.a(System.lineSeparator());
        ansi.a("in order to get a new, valid, enrollment token for new elasticsearch nodes.");
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        ansi.boldOff();
        ansi.a("-".repeat(Math.max(1, out.getTerminalWidth())));
        ansi.a(System.lineSeparator());
        ansi.a(System.lineSeparator());
        out.println(ansi);
    }

    private void outputOnError(@Nullable Exception e) {
        // TODO
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
