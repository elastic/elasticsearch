/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

// @formatter:off
@SuppressWarnings({"requires-automatic","module"})
module org.elasticsearch.core {
    requires static /*transitive*/ jsr305; // ####: does this need to be transitive

    exports org.elasticsearch.core;
    exports org.elasticsearch.jdk;

    // java.lang.IllegalAccessError: class org.elasticsearch.xpack.core.ilm.LifecyclePolicyUtils
    // (in unnamed module @0x4b691611) cannot access class org.elasticsearch.core.internal.io.Streams
    // (in module org.elasticsearch.core) because module org.elasticsearch.core does not export
    // org.elasticsearch.core.internal.io to unnamed module
    // Why is this accessing internal.io ?
    exports org.elasticsearch.core.internal.io; // to org.elasticsearch.xcontent, org.elasticsearch.cli, org.elasticsearch.server;

    // class org.elasticsearch.transport.netty4.Netty4Transport$ClientChannelInitializer
    // (in unnamed module @0x3f570944) cannot access class org.elasticsearch.core.internal.net.NetUtils
    // (in module org.elasticsearch.core) because module org.elasticsearch.core does not export
    // org.elasticsearch.core.internal.net to unnamed module
    exports org.elasticsearch.core.internal.net; // to org.elasticsearch.xcontent;
}
// @formatter:on
