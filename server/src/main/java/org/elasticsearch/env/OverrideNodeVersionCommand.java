/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.env;

import joptsimple.OptionParser;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.cluster.coordination.ElasticsearchNodeCommand;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;

public class OverrideNodeVersionCommand extends ElasticsearchNodeCommand {
    private static final Logger logger = LogManager.getLogger(OverrideNodeVersionCommand.class);

    private static final String TOO_NEW_MESSAGE =
        DELIMITER +
            "\n" +
            "This data path was last written by Elasticsearch version [V_NEW] and may no\n" +
            "longer be compatible with Elasticsearch version [V_CUR]. This tool will bypass\n" +
            "this compatibility check, allowing a version [V_CUR] node to start on this data\n" +
            "path, but a version [V_CUR] node may not be able to read this data or may read\n" +
            "it incorrectly leading to data loss.\n" +
            "\n" +
            "You should not use this tool. Instead, continue to use a version [V_NEW] node\n" +
            "on this data path. If necessary, you can use reindex-from-remote to copy the\n" +
            "data from here into an older cluster.\n" +
            "\n" +
            "Do you want to proceed?\n";

    private static final String TOO_OLD_MESSAGE =
        DELIMITER +
            "\n" +
            "This data path was last written by Elasticsearch version [V_OLD] which may be\n" +
            "too old to be readable by Elasticsearch version [V_CUR].  This tool will bypass\n" +
            "this compatibility check, allowing a version [V_CUR] node to start on this data\n" +
            "path, but this version [V_CUR] node may not be able to read this data or may\n" +
            "read it incorrectly leading to data loss.\n" +
            "\n" +
            "You should not use this tool. Instead, upgrade this data path from [V_OLD] to\n" +
            "[V_CUR] using one or more intermediate versions of Elasticsearch.\n" +
            "\n" +
            "Do you want to proceed?\n";

    static final String NO_METADATA_MESSAGE = "no node metadata found, so there is no version to override";
    static final String SUCCESS_MESSAGE = "Successfully overwrote this node's metadata to bypass its version compatibility checks.";

    public OverrideNodeVersionCommand() {
        super("Overwrite the version stored in this node's data path with [" + Version.CURRENT +
            "] to bypass the version compatibility checks");
    }

    @Override
    protected void processNodePaths(Terminal terminal, Path[] dataPaths, Environment env) throws IOException {
        final Path[] nodePaths = Arrays.stream(toNodePaths(dataPaths)).map(p -> p.path).toArray(Path[]::new);
        final NodeMetaData nodeMetaData
            = new NodeMetaData.NodeMetaDataStateFormat(true).loadLatestState(logger, namedXContentRegistry, nodePaths);
        if (nodeMetaData == null) {
            throw new ElasticsearchException(NO_METADATA_MESSAGE);
        }

        try {
            nodeMetaData.upgradeToCurrentVersion();
            throw new ElasticsearchException("found [" + nodeMetaData + "] which is compatible with current version [" + Version.CURRENT
                + "], so there is no need to override the version checks");
        } catch (IllegalStateException e) {
            // ok, means the version change is not supported
        }

        confirm(terminal, (nodeMetaData.nodeVersion().before(Version.CURRENT) ? TOO_OLD_MESSAGE : TOO_NEW_MESSAGE)
            .replace("V_OLD", nodeMetaData.nodeVersion().toString())
            .replace("V_NEW", nodeMetaData.nodeVersion().toString())
            .replace("V_CUR", Version.CURRENT.toString()));

        NodeMetaData.FORMAT.writeAndCleanup(new NodeMetaData(nodeMetaData.nodeId(), Version.CURRENT), nodePaths);

        terminal.println(SUCCESS_MESSAGE);
    }

    //package-private for testing
    OptionParser getParser() {
        return parser;
    }
}
