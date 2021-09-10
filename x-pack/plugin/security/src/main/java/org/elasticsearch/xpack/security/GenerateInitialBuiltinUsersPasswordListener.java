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
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.io.PrintStream;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.security.tool.CommandUtils.generatePassword;

public class GenerateInitialBuiltinUsersPasswordListener implements BiConsumer<SecurityIndexManager.State, SecurityIndexManager.State> {

    private static final Logger LOGGER  = LogManager.getLogger(GenerateInitialBuiltinUsersPasswordListener.class);
    private final NativeUsersStore nativeUsersStore;
    private final SecurityIndexManager securityIndexManager;

    public GenerateInitialBuiltinUsersPasswordListener(NativeUsersStore nativeUsersStore, SecurityIndexManager securityIndexManager) {
        this.nativeUsersStore = nativeUsersStore;
        this.securityIndexManager = securityIndexManager;
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
                                        outputOnSuccess(elasticPassword, kibanaSystemPassword, out);
                                    }, this::outputOnError
                                )
                            );
                    }, this::outputOnError));
            securityIndexManager.removeStateListener(this);
        }
    }

    private void outputOnSuccess(SecureString elasticPassword, SecureString kibanaSystemPassword, PrintStream out) {
        out.println();
        out.println("-----------------------------------------------------------------");
        out.println();
        out.println("Password for the elastic user is: " + elasticPassword);
        out.println();
        out.println("Password for the kibana_system user is: " + kibanaSystemPassword);
        out.println();
        out.println("Please note these down as they will not be shown again.");
        out.println();
        out.println();
        out.println("You can use 'bin/elasticsearch-reset-elastic-password' at any time");
        out.println("in order to reset the password for the elastic user.");
        out.println();
        out.println("You can use 'bin/elasticsearch-reset-kibana-system-password' at any time");
        out.println("in order to reset the password for the kibana_system user.");
        out.println();
        out.println("-----------------------------------------------------------------");
        out.println();
    }

    private void outputOnError(@Nullable Exception e) {
        if (e instanceof VersionConflictEngineException == false) {
            LOGGER.info("");
            LOGGER.info("-----------------------------------------------------------------");
            LOGGER.info("");
            LOGGER.info("Unable set the password for the elastic and kibana_system users ");
            LOGGER.info("automatically.");
            LOGGER.info("");
            LOGGER.info("You can use 'bin/elasticsearch-reset-elastic-password'");
            LOGGER.info("in order to set the password for the elastic user.");
            LOGGER.info("");
            LOGGER.info("You can use 'bin/elasticsearch-reset-kibana-system-password'");
            LOGGER.info("in order to set the password for the kibana_system user.");
            LOGGER.info("");
            LOGGER.info("-----------------------------------------------------------------");
            LOGGER.info("");
        }
        if (null != e)  {
            LOGGER.warn("Error initializing passwords for elastic and kibana_system users", e);
        }
    }
}
