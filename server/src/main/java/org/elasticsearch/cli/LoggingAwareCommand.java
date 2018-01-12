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

package org.elasticsearch.cli;

/**
 * A command that is aware of logging. This class should be preferred over the base {@link Command} class for any CLI tools that depend on
 * core Elasticsearch as they could directly or indirectly touch classes that touch logging and as such logging needs to be configured.
 */
public abstract class LoggingAwareCommand extends Command {

    /**
     * Construct the command with the specified command description. This command will have logging configured without reading Elasticsearch
     * configuration files.
     *
     * @param description the command description
     */
    public LoggingAwareCommand(final String description) {
        super(description, CommandLoggingConfigurator::configureLoggingWithoutConfig);
    }

}
