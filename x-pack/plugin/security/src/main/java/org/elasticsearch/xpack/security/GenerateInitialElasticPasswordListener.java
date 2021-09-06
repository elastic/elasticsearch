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
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.security.tool.CommandUtils.generatePassword;

public class GenerateInitialElasticPasswordListener implements BiConsumer<SecurityIndexManager.State, SecurityIndexManager.State> {

    private static final Logger LOGGER  = LogManager.getLogger(GenerateInitialElasticPasswordListener.class);
    private NativeUsersStore nativeUsersStore;
    private SecurityIndexManager securityIndexManager;

    public GenerateInitialElasticPasswordListener(NativeUsersStore nativeUsersStore, SecurityIndexManager securityIndexManager) {
        this.nativeUsersStore = nativeUsersStore;
        this.securityIndexManager = securityIndexManager;
    }

    @Override
    public void accept(SecurityIndexManager.State previousState, SecurityIndexManager.State currentState) {
        if (previousState.equals(SecurityIndexManager.State.UNRECOVERED_STATE)
            && currentState.equals(SecurityIndexManager.State.UNRECOVERED_STATE) == false
            && securityIndexManager.indexExists() == false) {

            final SecureString elasticPassword = new SecureString(generatePassword(20));
            nativeUsersStore
                .updateReservedUser(
                    ElasticUser.NAME,
                    elasticPassword.getChars(),
                    DocWriteRequest.OpType.CREATE,
                    WriteRequest.RefreshPolicy.IMMEDIATE,
                    ActionListener.wrap(
                        r -> {
                            LOGGER.info("");
                            LOGGER.info("-----------------------------------------------------------------");
                            LOGGER.info("");
                            LOGGER.info("");
                            LOGGER.info("");
                            LOGGER.info("Password for the elastic user is: ");
                            LOGGER.info(elasticPassword);
                            LOGGER.info("");
                            LOGGER.info("");
                            LOGGER.info("Please note this down as it will not be shown again.");
                            LOGGER.info("");
                            LOGGER.info("You can use 'bin/elasticsearch-reset-elastic-password' at any time");
                            LOGGER.info("in order to reset the password for the elastic user.");
                            LOGGER.info("");
                            LOGGER.info("");
                            LOGGER.info("");
                            LOGGER.info("-----------------------------------------------------------------");
                            LOGGER.info("");
                            securityIndexManager.removeStateListener(this);
                        },
                        e -> {
                            if (e instanceof VersionConflictEngineException == false) {
                                LOGGER.info("");
                                LOGGER.info("-----------------------------------------------------------------");
                                LOGGER.info("");
                                LOGGER.info("");
                                LOGGER.info("");
                                LOGGER.info("Failed to set the password for the elastic user automatically");
                                LOGGER.info("");
                                LOGGER.info("You can use 'bin/elasticsearch-reset-elastic-password'");
                                LOGGER.info("in order to set the password for the elastic user.");
                                LOGGER.info("");
                                LOGGER.info("");
                                LOGGER.info("");
                                LOGGER.info("-----------------------------------------------------------------");
                                LOGGER.info("");
                            }
                            LOGGER.warn(e);
                            securityIndexManager.removeStateListener(this);
                        }
                    )
                );
        }
    }
}
