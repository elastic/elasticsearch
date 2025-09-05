/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.entitlement.qa.entitled {
    requires org.elasticsearch.server;
    requires org.elasticsearch.entitlement;
    requires org.elasticsearch.base; // SuppressForbidden
    requires org.elasticsearch.logging;
    requires java.logging;

    exports org.elasticsearch.entitlement.qa.entitled; // Must be unqualified so non-modular IT tests can call us
}
