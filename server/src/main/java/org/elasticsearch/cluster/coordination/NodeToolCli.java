/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cli.MultiCommand;
import org.elasticsearch.env.NodeRepurposeCommand;
import org.elasticsearch.env.OverrideNodeVersionCommand;

class NodeToolCli extends MultiCommand {

    NodeToolCli() {
        super("A CLI tool to do unsafe cluster and index manipulations on current node");
        subcommands.put("repurpose", new NodeRepurposeCommand());
        subcommands.put("unsafe-bootstrap", new UnsafeBootstrapMasterCommand());
        subcommands.put("detach-cluster", new DetachClusterCommand());
        subcommands.put("override-version", new OverrideNodeVersionCommand());
        subcommands.put("remove-settings", new RemoveSettingsCommand());
        subcommands.put("remove-customs", new RemoveCustomsCommand());
    }
}
