/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli;

import joptsimple.OptionSet;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;

import java.nio.file.Files;

// this starts a single node cluster with security features enabled
public class AutoConfigInitialNode extends EnvironmentAwareCommand {

    public AutoConfigInitialNode() {
        super("Generates all the necessary configuration for the initial node of a new secure cluster");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        if (Files.isDirectory(env.dataFile())) {
            terminal.println(Terminal.Verbosity.VERBOSE,
                            "Skipping security auto configuration because it appears that the node is not starting up for the first time.");
            // TODO maybe throw a special error that informs the auto configuration skipped
            return;
        }
        if (env.settings().hasValue(XPackSettings.SECURITY_ENABLED.getKey()) ||
                false == env.settings().getByPrefix(XPackSettings.TRANSPORT_SSL_PREFIX).isEmpty()) {
            // do not try to validate, correct or fill in incomplete security configuration,
            // but instead rely on the node startup to do the validation
            terminal.println(Terminal.Verbosity.VERBOSE,
                    "Skipping security auto configuration because it appears that security is already configured.");
            // TODO maybe throw a special error that informs the auto configuration skipped
            return;
        }
        // TODO if cannot be leader (is not master)
        // TODO generate transport PKCS12 with password that is stored in the keystore


        // TODO if single-node
        // TODo check environment variable
        final boolean isSecurityEnabled = XPackSettings.SECURITY_ENABLED.get(env.settings());
        final boolean isSecurityExplicitlySet = env.settings().hasValue(XPackSettings.SECURITY_ENABLED.getKey());
        // don't try to do settings validation, only bare minimum necessary
        if (isSecurityExplicitlySet) {
            if (isSecurityEnabled) {

            } else {
                terminal.println("Security is disabled by configuration.");
                return;
            }
        } else {
            //

//            final ClusterState clusterState =
//                    loadTermAndClusterState(createPersistedClusterStateService(env.settings(), dataPath), environment).v2();
        }
    }

}
