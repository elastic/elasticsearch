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

package org.elasticsearch.index.translog;

import org.elasticsearch.cli.LoggingAwareMultiCommand;
import org.elasticsearch.cli.Terminal;
import org.elasticsearch.index.shard.RemoveCorruptedShardDataCommand;

/**
 * Class encapsulating and dispatching commands from the {@code elasticsearch-translog} command line tool
 */
@Deprecated
public class TranslogToolCli extends LoggingAwareMultiCommand {

    private TranslogToolCli() {
        // that's only for 6.x branch for bwc with elasticsearch-translog
        super("A CLI tool for various Elasticsearch translog actions");
        subcommands.put("truncate", new RemoveCorruptedShardDataCommand(true));
    }

    public static void main(String[] args) throws Exception {
        exit(new TranslogToolCli().main(args, Terminal.DEFAULT));
    }

}
