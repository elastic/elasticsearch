/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.sql.cli;

import org.jline.reader.Completer;
import org.jline.reader.impl.completer.AggregateCompleter;
import org.jline.reader.impl.completer.ArgumentCompleter;
import org.jline.reader.impl.completer.StringsCompleter;

class Completers {
    //TODO: need tree structure
    static final Completer INSTANCE = new AggregateCompleter(
            new ArgumentCompleter(new StringsCompleter("", "EXPLAIN", "SHOW", "SELECT", "SET")),
            new ArgumentCompleter(new StringsCompleter("SHOW", "TABLE", "COLUMNS", "FUNCTIONS")));

}
