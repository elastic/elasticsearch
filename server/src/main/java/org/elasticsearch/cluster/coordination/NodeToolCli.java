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

import org.elasticsearch.cli.CommandLoggingConfigurator;
import org.elasticsearch.cli.MultiCommand;
import org.elasticsearch.cli.Terminal;
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
        super("A CLI tool to do unsafe cluster and index manipulations on current node",
            ()->{});
        CommandLoggingConfigurator.configureLoggingWithoutConfig();
        subcommands.put("repurpose", new NodeRepurposeCommand());
        subcommands.put("unsafe-bootstrap", new UnsafeBootstrapMasterCommand());
        subcommands.put("detach-cluster", new DetachClusterCommand());
        subcommands.put("override-version", new OverrideNodeVersionCommand());
    }

    public static void main(String[] args) throws Exception {
        exit(new NodeToolCli().main(args, Terminal.DEFAULT));
    }

}
