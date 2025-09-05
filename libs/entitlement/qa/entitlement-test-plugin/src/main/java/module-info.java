/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.entitlement.qa.test {
    requires org.elasticsearch.server;
    requires org.elasticsearch.base;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.entitlement;
    requires org.elasticsearch.entitlement.qa.entitled;

    // Modules we'll attempt to use in order to exercise entitlements
    requires java.logging;
    requires java.net.http;
    requires jdk.net;
    requires java.desktop;
}
