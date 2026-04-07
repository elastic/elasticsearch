/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

// This module-info is used just to satisfy your IDE.
// At build and run time, the bridge is patched into the java.base module.
module org.elasticsearch.entitlement.bridge {
    requires java.net.http;
    requires jdk.net;
    requires java.logging;

    exports org.elasticsearch.entitlement.bridge;
}
