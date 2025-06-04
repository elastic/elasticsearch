/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.env;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.coordination.ElasticsearchNodeCommand;
import org.elasticsearch.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public class OverrideNodeVersionCommand extends ElasticsearchNodeCommand {
    private static final String TOO_NEW_MESSAGE = DELIMITER
        + "\n"
        + "This data path was last written by Elasticsearch version [V_NEW] and may no\n"
        + "longer be compatible with Elasticsearch version [V_CUR]. This tool will bypass\n"
        + "this compatibility check, allowing a version [V_CUR] node to start on this data\n"
        + "path, but a version [V_CUR] node may not be able to read this data or may read\n"
        + "it incorrectly leading to data loss.\n"
        + "\n"
        + "You should not use this tool. Instead, continue to use a version [V_NEW] node\n"
        + "on this data path. If necessary, you can use reindex-from-remote to copy the\n"
        + "data from here into an older cluster.\n"
        + "\n"
        + "Do you want to proceed?\n";

    private static final String TOO_OLD_MESSAGE = DELIMITER
        + "\n"
        + "This data path was last written by Elasticsearch version [V_OLD] which may be\n"
        + "too old to be readable by Elasticsearch version [V_CUR].  This tool will bypass\n"
        + "this compatibility check, allowing a version [V_CUR] node to start on this data\n"
        + "path, but this version [V_CUR] node may not be able to read this data or may\n"
        + "read it incorrectly leading to data loss.\n"
        + "\n"
        + "You should not use this tool. Instead, upgrade this data path from [V_OLD] to\n"
        + "[V_CUR] using one or more intermediate versions of Elasticsearch.\n"
        + "\n"
        + "Do you want to proceed?\n";

    static final String NO_METADATA_MESSAGE = "no node metadata found, so there is no version to override";
    static final String SUCCESS_MESSAGE = "Successfully overwrote this node's metadata to bypass its version compatibility checks.";

    public OverrideNodeVersionCommand() {
        super(
            "Overwrite the version stored in this node's data path with ["
                + Version.CURRENT
                + "] to bypass the version compatibility checks"
        );
    }

    @Override
    protected void processDataPaths(Terminal terminal, Path[] paths, OptionSet options, Environment env) throws IOException {
        final Path[] dataPaths = Arrays.stream(toDataPaths(paths)).map(p -> p.path).toArray(Path[]::new);
        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(dataPaths);
        if (nodeMetadata == null) {
            throw new ElasticsearchException(NO_METADATA_MESSAGE);
        }

        try {
            nodeMetadata.upgradeToCurrentVersion();
            throw new ElasticsearchException(
                "found ["
                    + nodeMetadata
                    + "] which is compatible with current version ["
                    + BuildVersion.current()
                    + "], so there is no need to override the version checks"
            );
        } catch (IllegalStateException e) {
            // ok, means the version change is not supported
        }

        confirm(
            terminal,
            (nodeMetadata.nodeVersion().onOrAfterMinimumCompatible() == false ? TOO_OLD_MESSAGE : TOO_NEW_MESSAGE).replace(
                "V_OLD",
                nodeMetadata.nodeVersion().toString()
            ).replace("V_NEW", nodeMetadata.nodeVersion().toString()).replace("V_CUR", BuildVersion.current().toString())
        );

        PersistedClusterStateService.overrideVersion(BuildVersion.current(), paths);

        terminal.println(SUCCESS_MESSAGE);
    }

    // package-private for testing
    OptionParser getParser() {
        return parser;
    }
}
