/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.cli.CommandLoggingConfigurator;
import org.elasticsearch.cli.LoggingAwareMultiCommand;
import org.elasticsearch.cli.Terminal;

public class SnapshotToolCli extends LoggingAwareMultiCommand {

    public SnapshotToolCli() {
        super("Tool to work with repositories and snapshots");
        CommandLoggingConfigurator.configureLoggingWithoutConfig();
        subcommands.put("cleanup_s3", new CleanupS3RepositoryCommand());
        subcommands.put("cleanup_gcs", new CleanupGCSRepositoryCommand());
    }

    public static void main(String[] args) throws Exception {
        exit(new SnapshotToolCli().main(args, Terminal.DEFAULT));
    }

}
