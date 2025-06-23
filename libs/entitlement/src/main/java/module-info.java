/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.entitlement {
    requires org.elasticsearch.base;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.logging;
    requires java.instrument;
    requires java.logging;
    requires java.net.http;
    requires jdk.attach;
    requires jdk.net;

    requires static org.elasticsearch.entitlement.bridge; // At runtime, this will be in java.base

    exports org.elasticsearch.entitlement.runtime.api;
    exports org.elasticsearch.entitlement.instrumentation;
    exports org.elasticsearch.entitlement.bootstrap to org.elasticsearch.server;
    exports org.elasticsearch.entitlement.initialization to java.base;

    // TODO: Most of the things in the policy package should be internal implementation details that are not exported.
    exports org.elasticsearch.entitlement.runtime.policy;
    exports org.elasticsearch.entitlement.runtime.policy.entitlements to org.elasticsearch.server;

    uses org.elasticsearch.entitlement.instrumentation.InstrumentationService;
}
