/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.security {
    requires org.elasticsearch.cli;
    requires org.elasticsearch.logging;
    requires org.elasticsearch.base;
    requires org.elasticsearch.server;
    requires org.elasticsearch.ssl.config;
    requires org.elasticsearch.transport.netty4;
    requires org.elasticsearch.xcontent;
    requires org.apache.lucene.core;
    requires org.elasticsearch.xcore;
    requires org.apache.lucene.queries;
    requires org.apache.lucene.sandbox;
    requires io.netty.handler;
    requires io.netty.transport;
    requires org.opensaml.core;
    requires org.opensaml.saml;
    requires org.opensaml.xmlsec.impl;
    requires slf4j.api;
    requires jopt.simple;
    requires java.naming;
    requires java.xml;
    requires java.security.jgss;
    requires unboundid.ldapsdk;

    exports org.elasticsearch.xpack.security.action to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.action.apikey to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.action.enrollment to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.action.oidc to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.action.privilege to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.action.profile to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.action.realm to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.action.role to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.action.rolemapping to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.action.saml to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.action.service to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.action.token to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.action.user to org.elasticsearch.server;

    exports org.elasticsearch.xpack.security.authc to org.elasticsearch.xcontent;
}
