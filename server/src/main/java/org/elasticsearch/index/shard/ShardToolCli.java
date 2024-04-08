/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.shard;

import org.elasticsearch.cli.MultiCommand;

/**
 * Class encapsulating and dispatching commands from the {@code elasticsearch-shard} command line tool
 */
class ShardToolCli extends MultiCommand {

    ShardToolCli() {
        super("A CLI tool to remove corrupted parts of unrecoverable shards");
        subcommands.put("remove-corrupted-data", new RemoveCorruptedShardDataCommand());
    }
}
