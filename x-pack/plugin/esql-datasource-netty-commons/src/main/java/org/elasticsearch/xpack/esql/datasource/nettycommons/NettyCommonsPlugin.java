/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.nettycommons;

import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;

/**
 * Shared Netty libraries plugin for ESQL external data source plugins.
 *
 * <p>This plugin acts as a classloader anchor — it bundles the Netty libraries
 * so that storage-provider plugins (S3, Azure) can access them through the
 * {@code extendedPlugins} mechanism without each bundling their own copy.
 *
 * <p>Plugins that need Netty declare
 * {@code extendedPlugins = ['x-pack-esql', 'esql-datasource-netty-commons']}
 * to inherit these libraries through parent-first classloader delegation.
 *
 * <p>This plugin intentionally does not extend {@code x-pack-esql} or
 * {@code transport-netty4} to avoid the {@code ssl-config} jar-hell diamond
 * that would propagate through transitive URL checks when consumers also
 * extend the {@code x-pack-esql} chain (which includes {@code x-pack-core}
 * and its own copy of {@code ssl-config}).
 */
public class NettyCommonsPlugin extends Plugin implements ExtensiblePlugin {
    // Classloader anchor only — no functionality beyond providing Netty on the classpath.
}
