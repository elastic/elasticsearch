/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

module org.elasticsearch.security {
    requires java.naming;
    requires java.security.jgss;
    requires java.xml;

    requires org.elasticsearch.base;
    requires org.elasticsearch.cli;
    requires org.elasticsearch.server;
    requires org.elasticsearch.sslconfig;
    requires org.elasticsearch.transport.netty4;
    requires org.elasticsearch.xcontent;
    requires org.elasticsearch.xcore;

    requires org.apache.commons.codec;
    requires org.apache.httpcomponents.httpcore;
    requires org.apache.httpcomponents.httpclient;
    requires org.apache.httpcomponents.httpasyncclient;
    requires org.apache.httpcomponents.httpcore.nio;
    requires org.apache.log4j;
    requires org.apache.logging.log4j;
    requires org.apache.logging.log4j.core;
    requires org.apache.lucene.core;
    requires org.apache.lucene.queries;
    requires org.apache.lucene.sandbox;

    requires org.opensaml.core;
    requires org.opensaml.saml;
    requires org.opensaml.saml.impl;
    requires org.opensaml.security;
    requires org.opensaml.security.impl;
    requires org.opensaml.xmlsec.impl;
    requires org.opensaml.xmlsec;

    requires com.nimbusds.jose.jwt;
    requires io.netty.common;
    requires io.netty.codec.http;
    requires io.netty.handler;
    requires io.netty.transport;
    requires jopt.simple;
    requires json.smart;
    requires net.shibboleth.utilities.java.support;
    requires oauth2.oidc.sdk;
    requires org.slf4j;
    requires unboundid.ldapsdk;
    requires org.elasticsearch.logging;

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
    exports org.elasticsearch.xpack.security.action.settings to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.operator to org.elasticsearch.internal.operator, org.elasticsearch.internal.security;
    exports org.elasticsearch.xpack.security.authz to org.elasticsearch.internal.security;
    exports org.elasticsearch.xpack.security.authc to org.elasticsearch.xcontent, org.elasticsearch.internal.security;
    exports org.elasticsearch.xpack.security.authc.saml to org.elasticsearch.internal.security;
    exports org.elasticsearch.xpack.security.slowlog to org.elasticsearch.server;
    exports org.elasticsearch.xpack.security.authc.support to org.elasticsearch.internal.security;
    exports org.elasticsearch.xpack.security.rest.action.apikey to org.elasticsearch.internal.security;
    exports org.elasticsearch.xpack.security.support to org.elasticsearch.internal.security;
    exports org.elasticsearch.xpack.security.authz.store to org.elasticsearch.internal.security;
    exports org.elasticsearch.xpack.security.authc.service;

    provides org.elasticsearch.index.SlowLogFieldProvider with org.elasticsearch.xpack.security.slowlog.SecuritySlowLogFieldProvider;

    provides org.elasticsearch.cli.CliToolProvider
        with
            org.elasticsearch.xpack.security.enrollment.tool.CreateEnrollmentTokenToolProvider,
            org.elasticsearch.xpack.security.authc.esnative.tool.ResetPasswordToolProvider,
            org.elasticsearch.xpack.security.authc.esnative.tool.SetupPasswordToolProvider,
            org.elasticsearch.xpack.security.authc.saml.SamlMetadataToolProvider,
            org.elasticsearch.xpack.security.authc.service.FileTokensToolProvider,
            org.elasticsearch.xpack.security.crypto.tool.SystemKeyToolProvider,
            org.elasticsearch.xpack.security.authc.file.tool.UsersToolProvider,
            org.elasticsearch.xpack.security.enrollment.tool.AutoConfigGenerateElasticPasswordHashToolProvider;

    provides org.elasticsearch.reservedstate.ReservedClusterStateHandlerProvider
        with
            org.elasticsearch.xpack.security.ReservedSecurityStateHandlerProvider;

    provides org.elasticsearch.features.FeatureSpecification with org.elasticsearch.xpack.security.SecurityFeatures;
}
