/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cli.MultiCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.common.cli.CommandLoggingConfigurator;
import org.elasticsearch.env.NodeRepurposeCommand;
import org.elasticsearch.env.OverrideNodeVersionCommand;

// NodeToolCli does not extend LoggingAwareCommand, because LoggingAwareCommand performs logging initialization
// after LoggingAwareCommand instance is constructed.
// It's too late for us, because before UnsafeBootstrapMasterCommand is added to the list of subcommands
// log4j2 initialization will happen, because it has static reference to Logger class.
// Even if we avoid making a static reference to Logger class, there is no nice way to avoid declaring
// UNSAFE_BOOTSTRAP, which depends on ClusterService, which in turn has static Logger.
// TODO execute CommandLoggingConfigurator.configureLoggingWithoutConfig() in the constructor of commands, not in beforeMain
public class NodeToolCli extends MultiCommand {

    public NodeToolCli() {
        super("A CLI tool to do unsafe cluster and index manipulations on current node", () -> {});
        CommandLoggingConfigurator.configureLoggingWithoutConfig();
        subcommands.put("repurpose", new NodeRepurposeCommand());
        subcommands.put("unsafe-bootstrap", new UnsafeBootstrapMasterCommand());
        subcommands.put("detach-cluster", new DetachClusterCommand());
        subcommands.put("override-version", new OverrideNodeVersionCommand());
        subcommands.put("remove-settings", new RemoveSettingsCommand());
        subcommands.put("remove-customs", new RemoveCustomsCommand());
    }

    public static void main(String[] args) throws Exception {
        exit(new NodeToolCli().main(args, Terminal.DEFAULT));
    }

}
