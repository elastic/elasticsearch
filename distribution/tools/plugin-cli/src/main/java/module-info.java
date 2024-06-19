/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import org.elasticsearch.cli.CliToolProvider;

module org.elasticsearch.plugins.cli {
    requires jopt.simple;
    requires org.apache.lucene.core;
    requires org.apache.lucene.suggest;
    requires org.bouncycastle.fips.core;
    requires org.bouncycastle.pg;
    requires org.elasticsearch.base;
    requires org.elasticsearch.cli;
    requires org.elasticsearch.plugin.scanner;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcontent;
    requires org.objectweb.asm;

    provides CliToolProvider with org.elasticsearch.plugins.cli.PluginCliProvider, org.elasticsearch.plugins.cli.SyncPluginsCliProvider;
}
