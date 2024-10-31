/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

module org.elasticsearch.libs.entitlement.tools.securitymanager.scanner {
    requires org.elasticsearch.libs.entitlement.tools.common;
    requires org.objectweb.asm.util;

    requires jdk.dynalink;
    requires java.sql;
    requires java.sql.rowset;
    requires java.net.http;
    requires jdk.unsupported;
    requires org.elasticsearch.base;
}
