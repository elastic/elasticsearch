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
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.xpack.core.security.EnrollmentToken;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.enrollment.EnrollmentTokenGenerator;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.PrintStream;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.security.authc.esnative.ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD;
import static org.elasticsearch.xpack.security.tool.CommandUtils.generatePassword;

public class InitialSecurityConfigurationListener implements BiConsumer<SecurityIndexManager.State, SecurityIndexManager.State> {

    private static final Logger LOGGER  = LogManager.getLogger(InitialSecurityConfigurationListener.class);
    private final NativeUsersStore nativeUsersStore;
    private final SecurityIndexManager securityIndexManager;
    private final Environment environment;

    public InitialSecurityConfigurationListener(
        NativeUsersStore nativeUsersStore,
        SecurityIndexManager securityIndexManager,
        Environment environment
    ) {
        this.nativeUsersStore = nativeUsersStore;
        this.securityIndexManager = securityIndexManager;
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
            && securityIndexManager.indexExists() == false) {
            if (BOOTSTRAP_ELASTIC_PASSWORD.exists(environment.settings())) {
                final SecureString elasticPassword = new SecureString(generatePassword(20));
                nativeUsersStore.updateReservedUser(
                    ElasticUser.NAME,
                    elasticPassword.getChars(),
                    DocWriteRequest.OpType.CREATE,
                    WriteRequest.RefreshPolicy.IMMEDIATE,
                    ActionListener.wrap(result -> {
                        try {
                            final EnrollmentTokenGenerator enrollmentTokenGenerator = new EnrollmentTokenGenerator(environment);
                            final EnrollmentToken enrollmentToken = enrollmentTokenGenerator.createKibanaEnrollmentToken(
                                ElasticUser.NAME,
                                elasticPassword
                            );
                            final String encodedToken = enrollmentToken.getEncoded();
                            final String httpCaFingerprint = enrollmentToken.getFingerprint();
                            outputAllInformationOnSuccess(elasticPassword, encodedToken, httpCaFingerprint, out);
                        } catch (Exception e) {
                            outputOnError(e);
                        }
                    }, this::outputOnError));
            } else {
                try {
                    final EnrollmentTokenGenerator enrollmentTokenGenerator = new EnrollmentTokenGenerator(environment);
                    final EnrollmentToken enrollmentToken = enrollmentTokenGenerator.createKibanaEnrollmentToken(
                        ElasticUser.NAME,
                        BOOTSTRAP_ELASTIC_PASSWORD.get(environment.settings())
                    );
                    final String encodedToken = enrollmentToken.getEncoded();
                    final String httpCaFingerprint = enrollmentToken.getFingerprint();
                    outputEnrollmentTokenOnSuccess(encodedToken, httpCaFingerprint, out);
                } catch (Exception e) {
                    outputOnError(e);
                }
            }
            securityIndexManager.removeStateListener(this);
        }
    }

    private void outputAllInformationOnSuccess(SecureString elasticPassword, String enrollmentToken, String httpCaFingerprint, PrintStream out) {
        out.println();
        out.println("-----------------------------------------------------------------");
        out.println();
        out.println("Password for the elastic user is: " + elasticPassword);
        out.println();
        out.println("Enrollment token for kibana:");
        out.println(enrollmentToken);
        out.println();
        out.println("Fingerprint of the generated CA certificate for HTTP:");
        out.println(httpCaFingerprint);
        out.println();
        out.println("Please note these down as they will not be shown again.");
        out.println();
        out.println();
        out.println("You can use 'bin/elasticsearch-reset-elastic-password' at any time");
        out.println("in order to reset the password for the elastic user.");
        out.println();
        out.println("The enrollment token for kibana is valid for the next 30 minutes.");
        out.println("After that, you can use");
        out.println("'bin/elasticsearch-create-enrollment-token -s kibana' at any time");
        out.println("in order to get a new, valid, enrollment token.");
        out.println();
        out.println("-----------------------------------------------------------------");
        out.println();
    }

    private void outputEnrollmentTokenOnSuccess(String enrollmentToken, String httpCaFingerprint, PrintStream out) {
        out.println();
        out.println("-----------------------------------------------------------------");
        out.println();
        out.println("Enrollment token for kibana:");
        out.println(enrollmentToken);
        out.println();
        out.println("Fingerprint of the generated CA certificate for HTTP:");
        out.println(httpCaFingerprint);
        out.println();
        out.println("Please note these down as they will not be shown again.");
        out.println();
        out.println();
        out.println("You can use 'bin/elasticsearch-reset-elastic-password' at any time");
        out.println("in order to reset the password for the elastic user.");
        out.println();
        out.println("The enrollment token for kibana is valid for the next 30 minutes.");
        out.println("After that, you can use");
        out.println("'bin/elasticsearch-create-enrollment-token -s kibana' at any time");
        out.println("in order to get a new, valid, enrollment token.");
        out.println();
        out.println("-----------------------------------------------------------------");
        out.println();
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
            LOGGER.info("-----------------------------------------------------------------");
            LOGGER.info("");
        }
        if (null != e)  {
            LOGGER.warn("Error setting initial password for elastic and generating a kibana enrollment token", e);
        }
    }
}
