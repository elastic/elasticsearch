/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

import org.elasticsearch.xpack.security.authz.microsoft.MicrosoftGraphAuthzPlugin;

module org.elasticsearch.plugin.security.authz {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.logging;
    requires org.apache.httpcomponents.httpclient;
    requires org.apache.httpcomponents.httpcore;
    requires com.nimbusds.jose.jwt;
    requires com.microsoft.kiota;
    requires com.microsoft.graph;
    requires com.azure.identity;
    requires com.microsoft.graph.core;
    requires kotlin.stdlib;
    requires com.google.gson;

    provides org.elasticsearch.xpack.core.security.SecurityExtension with MicrosoftGraphAuthzPlugin;
}
