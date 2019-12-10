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
package org.elasticsearch.cluster.coordination;

import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cli.EnvironmentAwareCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;

public abstract class ElasticsearchNodeCommand extends EnvironmentAwareCommand {
    protected static final String DELIMITER = "------------------------------------------------------------------------\n";

    static final String STOP_WARNING_MSG =
            DELIMITER +
                    "\n" +
                    "    WARNING: Elasticsearch MUST be stopped before running this tool." +
                    "\n";
    protected static final String FAILED_TO_OBTAIN_NODE_LOCK_MSG = "failed to lock node's directory, is Elasticsearch still running?";
    protected static final String NO_NODE_FOLDER_FOUND_MSG = "no node folder is found in data folder(s), node has not been started yet?";
    protected static final String CS_MISSING_MSG =
        "cluster state is empty, cluster has never been bootstrapped?";
    protected static final String ABORTED_BY_USER_MSG = "aborted by user";

    public ElasticsearchNodeCommand(String description) {
        super(description);
    }

    protected void processNodePaths(Terminal terminal, OptionSet options, Environment env) throws IOException {
        terminal.println(Terminal.Verbosity.VERBOSE, "Obtaining lock for node");
        try (NodeEnvironment.NodeLock lock = new NodeEnvironment.NodeLock(logger, env, Files::exists)) {
            final Path[] dataPaths =
                    Arrays.stream(lock.getNodePaths()).filter(Objects::nonNull).map(p -> p.path).toArray(Path[]::new);
            if (dataPaths.length == 0) {
                throw new ElasticsearchException(NO_NODE_FOLDER_FOUND_MSG);
            }
            processNodePaths(terminal, dataPaths, options, env);
        } catch (LockObtainFailedException e) {
            throw new ElasticsearchException(FAILED_TO_OBTAIN_NODE_LOCK_MSG, e);
        }
    }

    protected void confirm(Terminal terminal, String msg) {
        terminal.println(msg);
        String text = terminal.readText("Confirm [y/N] ");
        if (text.equalsIgnoreCase("y") == false) {
            throw new ElasticsearchException(ABORTED_BY_USER_MSG);
        }
    }

    @Override
    public final void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        terminal.println(STOP_WARNING_MSG);
        if (validateBeforeLock(terminal, env)) {
            processNodePaths(terminal, options, env);
        }
    }

    /**
     * Validate that the command can run before taking any locks.
     * @param terminal the terminal to print to
     * @param env the env to validate.
     * @return true to continue, false to stop (must print message in validate).
     */
    protected boolean validateBeforeLock(Terminal terminal, Environment env) {
        return true;
    }


    /**
     * Process the paths. Locks for the paths is held during this method invocation.
     * @param terminal the terminal to use for messages
     * @param dataPaths the paths of the node to process
     * @param options the command line options
     * @param env the env of the node to process
     */
    protected abstract void processNodePaths(Terminal terminal, Path[] dataPaths, OptionSet options, Environment env) throws IOException;

    public final void processNodePaths(Terminal terminal, Path[] dataPaths, Environment env) throws IOException {
        processNodePaths(terminal, dataPaths, new OptionParser().parse(), env);
    }

    protected NodeEnvironment.NodePath[] toNodePaths(Path[] dataPaths) {
        return Arrays.stream(dataPaths).map(ElasticsearchNodeCommand::createNodePath).toArray(NodeEnvironment.NodePath[]::new);
    }

    private static NodeEnvironment.NodePath createNodePath(Path path) {
        try {
            return new NodeEnvironment.NodePath(path);
        } catch (IOException e) {
            throw new ElasticsearchException("Unable to investigate path [" + path + "]", e);
        }
    }

    //package-private for testing
    OptionParser getParser() {
        return parser;
    }
}
