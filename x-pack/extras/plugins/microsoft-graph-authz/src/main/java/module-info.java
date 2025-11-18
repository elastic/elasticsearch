/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

import org.elasticsearch.xpack.security.authz.microsoft.MicrosoftGraphAuthzPlugin;

module org.elasticsearch.plugin.security.authz {
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.xcore;
    requires org.elasticsearch.logging;
    requires org.apache.httpcomponents.httpclient;
    requires org.apache.httpcomponents.httpcore;
    requires com.microsoft.kiota;
    requires com.microsoft.graph;
    requires com.azure.identity;
    requires com.microsoft.graph.core;
    requires kotlin.stdlib;
    requires com.google.gson;
    requires okhttp3;
    requires com.azure.core.http.okhttp;
    requires org.apache.logging.log4j;

    provides org.elasticsearch.xpack.core.security.SecurityExtension with MicrosoftGraphAuthzPlugin;
}
