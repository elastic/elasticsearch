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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.security.tool.CommandUtils.generatePassword;

public class GenerateInitialBuiltinUsersPasswordListener implements BiConsumer<SecurityIndexManager.State, SecurityIndexManager.State> {

    private static final Logger LOGGER  = LogManager.getLogger(GenerateInitialBuiltinUsersPasswordListener.class);
    private NativeUsersStore nativeUsersStore;
    private SecurityIndexManager securityIndexManager;

    public GenerateInitialBuiltinUsersPasswordListener(NativeUsersStore nativeUsersStore, SecurityIndexManager securityIndexManager) {
        this.nativeUsersStore = nativeUsersStore;
        this.securityIndexManager = securityIndexManager;
    }

    @Override
    public void accept(SecurityIndexManager.State previousState, SecurityIndexManager.State currentState) {
        if (previousState.equals(SecurityIndexManager.State.UNRECOVERED_STATE)
            && currentState.equals(SecurityIndexManager.State.UNRECOVERED_STATE) == false
            && securityIndexManager.indexExists() == false) {

            final SecureString elasticPassword = new SecureString(generatePassword(20));
            final SecureString kibanaSystemPassword = new SecureString(generatePassword(20));
            nativeUsersStore
                .updateReservedUser(
                    ElasticUser.NAME,
                    elasticPassword.getChars(),
                    DocWriteRequest.OpType.CREATE,
                    WriteRequest.RefreshPolicy.IMMEDIATE,
                    ActionListener.wrap(result -> {
                        nativeUsersStore
                            .updateReservedUser(
                                KibanaSystemUser.NAME,
                                kibanaSystemPassword.getChars(),
                                DocWriteRequest.OpType.CREATE,
                                WriteRequest.RefreshPolicy.IMMEDIATE,
                                ActionListener.wrap(
                                    r -> {
                                        outputOnSuccess(elasticPassword, kibanaSystemPassword);
                                    }, this::outputOnError
                                )
                            );
                    }, this::outputOnError));
            securityIndexManager.removeStateListener(this);
        }
    }

    private void outputOnSuccess(SecureString elasticPassword, SecureString kibanaSystemPassword) {
        LOGGER.info("");
        LOGGER.info("-----------------------------------------------------------------");
        LOGGER.info("");
        LOGGER.info("");
        LOGGER.info("");
        LOGGER.info("Password for the elastic user is: " + elasticPassword);
        LOGGER.info("");
        LOGGER.info("");
        LOGGER.info("");
        LOGGER.info("Password for the kibana_system user is: " + kibanaSystemPassword);
        LOGGER.info("");
        LOGGER.info("");
        LOGGER.info("Please note these down as they will not be shown again.");
        LOGGER.info("");
        LOGGER.info("You can use 'bin/elasticsearch-reset-elastic-password' at any time");
        LOGGER.info("in order to reset the password for the elastic user.");
        LOGGER.info("");
        LOGGER.info("");
        LOGGER.info("You can use 'bin/elasticsearch-reset-kibana-system-password' at any time");
        LOGGER.info("in order to reset the password for the kibana_system user.");
        LOGGER.info("");
        LOGGER.info("");
        LOGGER.info("");
        LOGGER.info("-----------------------------------------------------------------");
        LOGGER.info("");
    }

    private void outputOnError(Exception e) {
        if (e instanceof VersionConflictEngineException == false) {
            LOGGER.info("");
            LOGGER.info("-----------------------------------------------------------------");
            LOGGER.info("");
            LOGGER.info("");
            LOGGER.info("");
            LOGGER.info("Failed to set the password for the elastic and kibana-system users ");
            LOGGER.info("automatically");
            LOGGER.info("");
            LOGGER.info("You can use 'bin/elasticsearch-reset-elastic-password'");
            LOGGER.info("in order to set the password for the elastic user.");
            LOGGER.info("");
            LOGGER.info("");
            LOGGER.info("You can use 'bin/elasticsearch-reset-kibana-system-password'");
            LOGGER.info("in order to set the password for the kibana_system user.");
            LOGGER.info("");
            LOGGER.info("");
            LOGGER.info("");
            LOGGER.info("-----------------------------------------------------------------");
            LOGGER.info("");
        }
    }
}
