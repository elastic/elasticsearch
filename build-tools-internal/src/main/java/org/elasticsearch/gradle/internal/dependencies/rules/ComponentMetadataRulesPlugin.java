/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.dependencies.rules;

import org.gradle.api.Plugin;
import org.gradle.api.artifacts.dsl.ComponentMetadataHandler;
import org.gradle.api.initialization.Settings;

import java.util.List;

/**
 * A settings plugin that configures component metadata rules for dependency management.
 * This plugin centralizes the configuration of transitive dependency exclusion rules.
 * Since we want to use the same rules in serverless an stack setup we cannot just put
 * this into settings.gradle but encapsulate it in a plugin that can be reused in the
 * context of serverless.
 */
public class ComponentMetadataRulesPlugin implements Plugin<Settings> {

    @Override
    public void apply(Settings settings) {
        ComponentMetadataHandler components = settings.getDependencyResolutionManagement().getComponents();

        // Azure dependencies
        components.withModule("com.azure:azure-core", ExcludeOtherGroupsTransitiveRule.class);
        // brings in azure-core-http-netty. not used
        components.withModule("com.azure:azure-core-http-netty", ExcludeAllTransitivesRule.class);
        components.withModule("com.azure:azure-core-http-okhttp", ExcludeOtherGroupsTransitiveRule.class);

        // brings in net.java.dev.jna:jna-platform:5.6.0. We use 5.12.1.
        // brings in com.azure:azure-core-http-netty:1.15.1
        components.withModule("com.azure:azure-identity", ExcludeAllTransitivesRule.class);

        // brings in com.fasterxml.jackson.dataformat:jackson-dataformat-xml:2.17.2. We use 2.15.0
        components.withModule("com.azure:azure-storage-blob", ExcludeOtherGroupsTransitiveRule.class);
        components.withModule("com.azure:azure-storage-blob-batch", ExcludeOtherGroupsTransitiveRule.class);
        components.withModule("com.azure:azure-storage-common", ExcludeOtherGroupsTransitiveRule.class);
        components.withModule("com.azure:azure-storage-internal-avro", ExcludeAllTransitivesRule.class);

        // Testing dependencies
        components.withModule("com.carrotsearch.randomizedtesting:randomizedtesting-runner", ExcludeAllTransitivesRule.class);

        // Jackson dependencies
        components.withModule("com.fasterxml.jackson.core:jackson-core", ExcludeOtherGroupsTransitiveRule.class);
        // brings in jackson-databind and jackson-annotations not used
        components.withModule("com.fasterxml.jackson.dataformat:jackson-dataformat-cbor", ExcludeAllTransitivesRule.class);
        components.withModule("com.fasterxml.jackson.dataformat:jackson-dataformat-smile", ExcludeAllTransitivesRule.class);

        // brings woodstox-core 6.7.0, we use 6.5.1
        components.withModule("com.fasterxml.jackson.dataformat:jackson-dataformat-xml", ExcludeOtherGroupsTransitiveRule.class);

        // brings in snakeyaml with wrong version
        components.withModule("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml", ExcludeOtherGroupsTransitiveRule.class);

        components.withModule("com.fasterxml.jackson.module:jackson-module-jaxb-annotations", ExcludeOtherGroupsTransitiveRule.class);

        // Google dependencies
        components.withModule("com.google.api-client:google-api-client", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.api.grpc:proto-google-cloud-storage-v2", ExcludeOtherGroupsTransitiveRule.class);

        // brings com.google.api.grpc:proto-google-common-protos:2.9.2 we
        components.withModule("com.google.api.grpc:proto-google-iam-v1", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.api:api-common", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.api:gax", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.api:gax-httpjson", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.apis:google-api-services-storage", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.auth:google-auth-library-oauth2-http", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.cloud:google-cloud-core", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.cloud:google-cloud-core-http", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.cloud:google-cloud-storage", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.code.gson:gson", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.guava:guava", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.http-client:google-http-client", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.http-client:google-http-client-appengine", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.http-client:google-http-client-gson", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.http-client:google-http-client-jackson2", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.jimfs:jimfs", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.oauth-client:google-oauth-client", ExcludeAllTransitivesRule.class);
        components.withModule("com.google.protobuf:protobuf-java-util", ExcludeAllTransitivesRule.class);
        components.withModule("com.googlecode.owasp-java-html-sanitizer:owasp-java-html-sanitizer", ExcludeAllTransitivesRule.class);

        // Other dependencies
        components.withModule("com.maxmind.geoip2:geoip2", ExcludeAllTransitivesRule.class);

        // Microsoft dependencies
        components.withModule("com.microsoft.azure:azure-core", ExcludeAllTransitivesRule.class);
        components.withModule("com.microsoft.azure:azure-svc-mgmt-compute", ExcludeAllTransitivesRule.class);
        components.withModule("com.microsoft.azure:msal4j", ExcludeAllTransitivesRule.class);
        components.withModule("com.microsoft.azure:msal4j-persistence-extension", ExcludeAllTransitivesRule.class);
        components.withModule("com.microsoft.graph:microsoft-graph", ExcludeAllTransitivesRule.class);
        components.withModule("com.microsoft.graph:microsoft-graph-core", ExcludeAllTransitivesRule.class);
        components.withModule("com.microsoft.kiota:microsoft-kiota-abstractions", ExcludeAllTransitivesRule.class);
        components.withModule("com.microsoft.kiota:microsoft-kiota-authentication-azure", ExcludeAllTransitivesRule.class);
        components.withModule("com.microsoft.kiota:microsoft-kiota-http-okHttp", ExcludeAllTransitivesRule.class);
        components.withModule("com.microsoft.kiota:microsoft-kiota-serialization-form", ExcludeAllTransitivesRule.class);
        components.withModule("com.microsoft.kiota:microsoft-kiota-serialization-json", ExcludeAllTransitivesRule.class);
        components.withModule("com.microsoft.kiota:microsoft-kiota-serialization-multipart", ExcludeAllTransitivesRule.class);
        components.withModule("com.microsoft.kiota:microsoft-kiota-serialization-text", ExcludeAllTransitivesRule.class);

        // Network dependencies
        components.withModule("com.networknt:json-schema-validator", ExcludeAllTransitivesRule.class);
        components.withModule("com.nimbusds:oauth2-oidc-sdk", ExcludeAllTransitivesRule.class);
        components.withModule("com.squareup.okhttp3:okhttp", ExcludeAllTransitivesRule.class);
        components.withModule("com.squareup.okio:okio", ExcludeAllTransitivesRule.class);
        components.withModule("com.squareup.okio:okio-jvm", ExcludeAllTransitivesRule.class);

        // Jakarta/javax mail dependencies
        components.withModule("com.sun.mail:jakarta.mail", ExcludeAllTransitivesRule.class);
        components.withModule("com.sun.xml.bind:jaxb-impl", ExcludeAllTransitivesRule.class);

        // Mapbox dependencies
        components.withModule("com.wdtinc:mapbox-vector-tile", ExcludeAllTransitivesRule.class);

        // Dropwizard dependencies
        components.withModule("io.dropwizard.metrics:metrics-core", ExcludeAllTransitivesRule.class);

        // gRPC dependencies
        components.withModule("io.grpc:grpc-api", ExcludeAllTransitivesRule.class);

        // Netty dependencies
        components.withModule("io.netty:netty-buffer", ExcludeAllTransitivesRule.class);
        components.withModule("io.netty:netty-codec", ExcludeAllTransitivesRule.class);
        components.withModule("io.netty:netty-codec-dns", ExcludeAllTransitivesRule.class);
        components.withModule("io.netty:netty-codec-http", ExcludeAllTransitivesRule.class);
        components.withModule("io.netty:netty-codec-http2", ExcludeAllTransitivesRule.class);
        components.withModule("io.netty:netty-codec-socks", ExcludeAllTransitivesRule.class);
        components.withModule("io.netty:netty-handler", ExcludeAllTransitivesRule.class);
        components.withModule("io.netty:netty-handler-proxy", ExcludeAllTransitivesRule.class);
        components.withModule("io.netty:netty-resolver", ExcludeAllTransitivesRule.class);
        components.withModule("io.netty:netty-resolver-dns", ExcludeAllTransitivesRule.class);
        components.withModule("io.netty:netty-transport", ExcludeAllTransitivesRule.class);
        components.withModule("io.netty:netty-transport-classes-epoll", ExcludeAllTransitivesRule.class);
        components.withModule("io.netty:netty-transport-native-unix-common", ExcludeAllTransitivesRule.class);

        // OpenCensus dependencies
        components.withModule("io.opencensus:opencensus-api", ExcludeAllTransitivesRule.class);
        components.withModule("io.opencensus:opencensus-contrib-http-util", ExcludeAllTransitivesRule.class);

        // OpenTelemetry dependencies
        components.withModule("io.opentelemetry:opentelemetry-api", ExcludeAllTransitivesRule.class);
        components.withModule("io.opentelemetry:opentelemetry-context", ExcludeAllTransitivesRule.class);
        components.withModule("io.opentelemetry:opentelemetry-exporter-common", ExcludeAllTransitivesRule.class);
        components.withModule("io.opentelemetry:opentelemetry-exporter-otlp", ExcludeAllTransitivesRule.class);
        components.withModule("io.opentelemetry:opentelemetry-exporter-otlp-common", ExcludeAllTransitivesRule.class);
        components.withModule("io.opentelemetry:opentelemetry-exporter-sender-jdk", ExcludeAllTransitivesRule.class);
        components.withModule("io.opentelemetry:opentelemetry-sdk", ExcludeAllTransitivesRule.class);
        components.withModule("io.opentelemetry:opentelemetry-sdk-common", ExcludeAllTransitivesRule.class);
        components.withModule("io.opentelemetry:opentelemetry-sdk-extension-autoconfigure", ExcludeAllTransitivesRule.class);
        components.withModule("io.opentelemetry:opentelemetry-sdk-extension-autoconfigure-spi", ExcludeAllTransitivesRule.class);
        components.withModule("io.opentelemetry:opentelemetry-sdk-metrics", ExcludeAllTransitivesRule.class);
        components.withModule("io.opentelemetry:opentelemetry-semconv", ExcludeAllTransitivesRule.class);

        // Project Reactor dependencies
        components.withModule("io.projectreactor.netty:reactor-netty-core", ExcludeAllTransitivesRule.class);
        components.withModule("io.projectreactor.netty:reactor-netty-http", ExcludeAllTransitivesRule.class);
        components.withModule("io.projectreactor:reactor-core", ExcludeAllTransitivesRule.class);

        // S2 Geometry dependencies
        components.withModule("io.sgr:s2-geometry-library-java", ExcludeAllTransitivesRule.class);

        // Jakarta XML dependencies
        components.withModule("jakarta.xml.bind:jakarta.xml.bind-api", ExcludeAllTransitivesRule.class);

        // javax mail dependencies
        components.withModule("javax.mail:mail", ExcludeAllTransitivesRule.class);
        components.withModule("javax.xml.bind:jaxb-api", ExcludeAllTransitivesRule.class);

        // Testing dependencies
        components.withModule("junit:junit", ExcludeAllTransitivesRule.class);

        // JNA dependencies
        components.withModule("net.java.dev.jna:jna-platform", ExcludeAllTransitivesRule.class);

        // JSON dependencies
        components.withModule("net.minidev:accessors-smart", ExcludeAllTransitivesRule.class);
        components.withModule("net.minidev:json-smart", ExcludeAllTransitivesRule.class);

        // EhCache dependencies
        components.withModule("net.sf.ehcache:ehcache", ExcludeAllTransitivesRule.class);

        // Shibboleth dependencies
        components.withModule("net.shibboleth.utilities:java-support", ExcludeAllTransitivesRule.class);

        // Apache Arrow dependencies
        components.withModule("org.apache.arrow:arrow-format", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.arrow:arrow-memory-core", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.arrow:arrow-vector", ExcludeAllTransitivesRule.class);

        // Apache Commons dependencies
        components.withModule("org.apache.commons:commons-text", ExcludeAllTransitivesRule.class);

        // org.apache.directory.api:api-asn1-ber brings in org.slf4j:slf4j-api:1.7.25. We use 2.0.6
        components.withModule("org.apache.directory.api:api-asn1-ber", ExcludeOtherGroupsTransitiveRule.class);

        // org.apache.directory.api:api-ldap-client-api brings in org.apache.mina:mina-core:2.0.16. We use 2.2.4
        components.withModule("org.apache.directory.api:api-ldap-client-api", ExcludeOtherGroupsTransitiveRule.class);
        components.withModule("org.apache.directory.api:api-ldap-codec-core", ExcludeAllTransitivesRule.class);

        // "org.apache.directory.api:api-ldap-codec-standalone brings in org.apache.mina:mina-core:2.0.16. We use 2.2.4
        components.withModule("org.apache.directory.api:api-ldap-codec-standalone", ExcludeOtherGroupsTransitiveRule.class);

        // TODO: For org.apache.directory.api dependencies we use partially 1.0.1 and partially 1.0.0. We should align these.
        components.withModule("org.apache.directory.api:api-ldap-extras-aci", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.directory.api:api-ldap-schema-data", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.directory.api:api-ldap-extras-codec-api", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.directory.api:api-ldap-extras-sp", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.directory.api:api-ldap-extras-util", ExcludeAllTransitivesRule.class);

        // org.apache.directory.api:api-ldap-model brings in org.apache.mina:mina-core:2.0.17. We use 2.2.4
        // org.apache.directory.api:api-ldap-model brings in org.apache.servicemix.bundles:org.apache.servicemix.bundles.antlr:2.7.7_5. We
        // use 2.7.7_5.
        // org.apache.directory.api:api-ldap-model brings in commons-codec:commons-lang:commons-lang:2.6. We use 2.6.
        // org.apache.directory.api:api-ldap-model brings in commons-collections:commons-collections:3.2.2. We use 3.3.2.
        // org.apache.directory.api:api-ldap-model brings in commons-codec:commons-codec:1.10. We use 1.15.
        // TODO exclude matching third party deps from being excluded
        components.withModule("org.apache.directory.api:api-ldap-model", ExcludeOtherGroupsTransitiveRule.class);

        // org.apache.directory.api:api-ldap-model brings in org.apache.mina:mina-core:2.0.17. We use 2.2.4
        components.withModule("org.apache.directory.api:api-ldap-net-mina", ExcludeOtherGroupsTransitiveRule.class);

        // org.apache.directory.api:api-asn1-ber brings in org.slf4j:slf4j-api:1.7.25. We use 2.0.6
        // TODO: For org.apache.directory.api dependencies we use partially 1.0.1 and partially 1.0.0. We should align these.
        components.withModule("org.apache.directory.api:api-util", ExcludeAllTransitivesRule.class);

        // Apache Directory Server dependencies
        components.withModule("org.apache.directory.jdbm:apacheds-jdbm1", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.directory.mavibot:mavibot", ExcludeAllTransitivesRule.class);
        // org.apache.directory.server:apacheds-core-annotations brings in org.apache.directory.api:api-ldap-model:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-core-annotations brings in org.apache.directory.api:api-util:1.0.0. We use 1.0.1.
        components.withModule("org.apache.directory.server:apacheds-core-annotations", ExcludeAllTransitivesRule.class);

        // brings in org.slf4j:slf4j-api:1.7.25. We use 2.0.6
        components.withModule("org.apache.directory.server:apacheds-core", ExcludeAllTransitivesRule.class);
        // brings in org.apache.directory.server:apacheds-core:2.0.0-M24
        components.withModule("org.apache.directory.server:apacheds-interceptor-kerberos", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.directory.server:apacheds-core-shared", ExcludeOtherGroupsTransitiveRule.class);
        // brings in org.apache.directory.server:apacheds-core-shared:2.0.0-M24
        components.withModule("org.apache.directory.server:apacheds-ldif-partition", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.directory.server:apacheds-protocol-shared", ExcludeOtherGroupsTransitiveRule.class);
        components.withModule("org.apache.directory.server:ldap-client-test", ExcludeOtherGroupsTransitiveRule.class);

        // org.apache.directory.server:apacheds-core-api brings in org.apache.directory.api:api-asn1-api:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-core-api brings in org.apache.directory.api:api-i18n:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-core-api brings in org.apache.directory.api:api-ldap-model:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-core-api brings in org.apache.directory.api:api-util:1.0.0. We use 1.0.1.
        components.withModule("org.apache.directory.server:apacheds-core-api", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.directory.server:apacheds-i18n", ExcludeAllTransitivesRule.class);
        // org.apache.directory.server:apacheds-jdbm-partition brings in org.apache.directory.api:api-ldap-model:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-jdbm-partition brings in org.apache.directory.api:api-util:1.0.0. We use 1.0.1.
        components.withModule("org.apache.directory.server:apacheds-jdbm-partition", ExcludeAllTransitivesRule.class);
        // org.apache.directory.server:apacheds-kerberos-codec brings in org.apache.directory.api:api-asn1-api:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-kerberos-codec brings in org.apache.directory.api:api-asn1-ber:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-kerberos-codec brings in org.apache.directory.api:api-i18n:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-kerberos-codec brings in org.apache.directory.api:api-ldap-model:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-kerberos-codec brings in org.apache.directory.api:api-util:1.0.0. We use 1.0.1.
        components.withModule("org.apache.directory.server:apacheds-kerberos-codec", ExcludeAllTransitivesRule.class);
        // org.apache.directory.server:apacheds-mavibot-partition brings in org.apache.directory.api:api-ldap-model:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-mavibot-partition brings in org.apache.directory.api:api-util:1.0.0. We use 1.0.1.
        components.withModule("org.apache.directory.server:apacheds-mavibot-partition", ExcludeAllTransitivesRule.class);
        // org.apache.directory.server:apacheds-protocol-kerberos brings in org.apache.directory.api:api-asn1-api:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-protocol-kerberos brings in org.apache.directory.api:api-ldap-model:1.0.0. We use 1.0.1.
        components.withModule("org.apache.directory.server:apacheds-protocol-kerberos", ExcludeAllTransitivesRule.class);
        // org.apache.directory.server:apacheds-protocol-ldap brings in org.apache.directory.api:api-asn1-ber:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-protocol-ldap brings in org.apache.directory.api:api-ldap-model:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-protocol-ldap brings in org.apache.directory.api:api-util:1.0.0. We use 1.0.1.
        components.withModule("org.apache.directory.server:apacheds-protocol-ldap", ExcludeAllTransitivesRule.class);
        // org.apache.directory.server:apacheds-server-annotations brings in org.apache.directory.api:api-ldap-model:1.0.0. We use 1.0.1.
        components.withModule("org.apache.directory.server:apacheds-server-annotations", ExcludeAllTransitivesRule.class);
        // org.apache.directory.server:apacheds-test-framework brings in org.apache.directory.api:api-ldap-model:1.0.0. We use 1.0.1.
        components.withModule("org.apache.directory.server:apacheds-test-framework", ExcludeAllTransitivesRule.class);
        // org.apache.directory.server:apacheds-xdbm-partition brings in org.apache.directory.api:api-ldap-model:1.0.0. We use 1.0.1.
        // org.apache.directory.server:apacheds-xdbm-partition brings in org.apache.directory.api:api-util:1.0.0. We use 1.0.1.
        components.withModule("org.apache.directory.server:apacheds-xdbm-partition", ExcludeAllTransitivesRule.class);

        // org.apache.directory.api:api-ldap-client-api brings in org.apache.mina:mina-core:2.0.16. We use 2.2.4
        components.withModule("org.apache.directory.server:apacheds-interceptors-authn", ExcludeAllTransitivesRule.class);

        // org.apache.tika:tika-core brings in org.slf4j:slf4j-api:1.17.36. We use 2.0.6
        components.withModule("org.apache.ftpserver:ftpserver-core", ExcludeByGroup.class, rule -> rule.params(List.of("org.slf4j")));

        // Hadoop dependencies
        components.withModule("org.apache.hadoop:hadoop-client-api", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.hadoop:hadoop-client-runtime", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.hadoop:hadoop-common", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.hadoop:hadoop-hdfs", ExcludeAllTransitivesRule.class);

        // Apache HTTP dependencies
        components.withModule("org.apache.httpcomponents.client5:httpclient5", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.httpcomponents:fluent-hc", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.httpcomponents:httpasyncclient", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.httpcomponents:httpclient", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.httpcomponents:httpclient-cache", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.httpcomponents:httpcore-nio", ExcludeAllTransitivesRule.class);

        // Apache James dependencies
        components.withModule("org.apache.james:apache-mime4j-core", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.james:apache-mime4j-dom", ExcludeAllTransitivesRule.class);

        // Apache Kerby dependencies
        components.withModule("org.apache.kerby:kerb-admin", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.kerby:kerb-client", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.kerby:kerb-common", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.kerby:kerb-identity", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.kerby:kerb-server", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.kerby:kerb-simplekdc", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.kerby:kerb-util", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.kerby:kerby-config", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.kerby:kerby-pkix", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.kerby:ldap-backend", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.kerby:token-provider", ExcludeAllTransitivesRule.class);

        // Apache Log4j dependencies
        // We want to remove log4j-api compile only dependency on biz.aQute.bnd and org.osgi group but
        // keep other compile only dependencies like spotbugs, errorprone and jspecify
        components.withModule("org.apache.logging.log4j:log4j-api", ExcludeByGroup.class, config -> {
            config.params(List.of("biz.aQute.bnd", "org.osgi"));
        });
        components.withModule("org.apache.logging.log4j:log4j-1.2-api", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.logging.log4j:log4j-core", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.logging.log4j:log4j-jcl", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.logging.log4j:log4j-slf4j-impl", ExcludeAllTransitivesRule.class);

        // lucene-analysis-morfologik brings in org.carrot2:morfologik-stemming:2.1.9. we use 2.1.1
        // lucene-analysis-morfologik brings in org.carrot2:morfologik-polish:2.1.9. we use none.
        // lucene-analysis-morfologik brings in ua.net.nlp:morfologik-ukrainian-search:4.9.1 we use 3.7.5.
        components.withModule("org.apache.lucene:lucene-analysis-morfologik", ExcludeOtherGroupsTransitiveRule.class);

        // lucene-analysis-phonetic brings in commons-codec:1.17. We use 1.15
        components.withModule("org.apache.lucene:lucene-analysis-phonetic", ExcludeOtherGroupsTransitiveRule.class);

        // lucene-spatial-extras brings in different version of spatial4j
        // lucene-spatial-extras brings in different version of s2-geometry-library-java
        components.withModule("org.apache.lucene:lucene-spatial-extras", ExcludeOtherGroupsTransitiveRule.class);

        // lucene-expressions brings in org.antlr:antlr4-runtime:4.13.2
        // lucene-expressions brings in org.ow2.asm:asm:9.6
        // lucene-expressions brings in org.ow2.asm:asm-commons:9.6
        components.withModule("org.apache.lucene:lucene-expressions", ExcludeOtherGroupsTransitiveRule.class);
        components.withModule("org.apache.lucene:lucene-test-framework", ExcludeOtherGroupsTransitiveRule.class);

        // Apache Mina dependencies
        components.withModule("org.apache.mina:mina-core", ExcludeAllTransitivesRule.class);

        // Apache PDFBox dependencies
        components.withModule("org.apache.pdfbox:fontbox", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.pdfbox:pdfbox", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.pdfbox:pdfbox-io", ExcludeAllTransitivesRule.class);

        // Apache POI dependencies
        components.withModule("org.apache.poi:poi", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.poi:poi-ooxml", ExcludeAllTransitivesRule.class);
        components.withModule("org.apache.poi:poi-scratchpad", ExcludeAllTransitivesRule.class);

        // Apache Santuario dependencies
        components.withModule("org.apache.santuario:xmlsec", ExcludeAllTransitivesRule.class);

        // org.apache.tika:tika-core brings in org.slf4j:slf4j-api:2.0.17. We use 2.0.6
        // org.apache.tika:tika-core brings in commons-io:commons-io:2.20.0. We use 2.5
        components.withModule("org.apache.tika:tika-core", ExcludeOtherGroupsTransitiveRule.class);

        // org.apache.tika:tika-langdetect-tika brings in com.optimaize.languagedetector:language-detector:0.6.
        components.withModule("org.apache.tika:tika-langdetect-tika", ExcludeOtherGroupsTransitiveRule.class);
        // org.apache.tika:tika-parser-apple-module brings in com.googlecode.plist:dd-plist:1.28.
        components.withModule("org.apache.tika:tika-parser-apple-module", ExcludeOtherGroupsTransitiveRule.class);
        // org.apache.tika:tika-parser-microsoft-module brings in com.healthmarketscience.jackcess:jackcess-encrypt:4.0.3.
        // org.apache.tika:tika-parser-microsoft-module brings in com.healthmarketscience.jackcess:jackcess:4.0.8.
        // org.apache.tika:tika-parser-microsoft-module brings in com.pff:java-libpst:0.9.3.
        // org.apache.tika:tika-parser-microsoft-module brings in commons-logging:commons-logging:1.3.5. We use 1.2.
        // org.apache.tika:tika-parser-microsoft-module brings in org.bouncycastle:bcjmail-jdk18on:1.81.
        // org.apache.tika:tika-parser-microsoft-module brings in org.bouncycastle:bcprov-jdk18on:1.81. We use 1.78.1/1.79.
        // org.apache.tika:tika-parser-microsoft-module brings in tika-parser-mail-commons.
        components.withModule("org.apache.tika:tika-parser-microsoft-module", ExcludeAllTransitivesRule.class);
        // org.apache.tika:tika-parser-miscoffice-module brings in org.glassfish.jaxb:jaxb-runtime:4.0.5.
        components.withModule("org.apache.tika:tika-parser-miscoffice-module", ExcludeOtherGroupsTransitiveRule.class);
        // org.apache.tika:tika-parser-pdf-module brings in org.apache.pdfbox:pdfbox-tools:3.0.5.
        // org.apache.tika:tika-parser-pdf-module brings in org.bouncycastle:bcjmail-jdk18on:1.81. Closest we use is
        // bcprov-jdk18on:1.78.1/1.79.
        // org.apache.tika:tika-parser-pdf-module brings in org.bouncycastle:bcprov-jdk18on:1.81. We use 1.78.1/1.79.
        // org.apache.tika:tika-parser-pdf-module brings in org.glassfish.jaxb:jaxb-runtime:4.0.5.
        components.withModule("org.apache.tika:tika-parser-pdf-module", ExcludeOtherGroupsTransitiveRule.class);
        // org.apache.tika:tika-parser-text-module brings in com.github.albfernandez:juniversalchardet:2.5.0..
        // org.apache.tika:tika-parser-text-module brings in org.apache.commons:commons-csv:1.14.1. We use 1.0.
        components.withModule("org.apache.tika:tika-parser-text-module", ExcludeOtherGroupsTransitiveRule.class);
        // org.apache.tika:tika-parser-xmp-commons brings in org.apache.pdfbox:xmpbox:3.0.5.
        components.withModule("org.apache.tika:tika-parser-xmp-commons", ExcludeOtherGroupsTransitiveRule.class);

        // Apache XMLBeans dependencies
        components.withModule("org.apache.xmlbeans:xmlbeans", ExcludeAllTransitivesRule.class);

        // BouncyCastle dependencies
        components.withModule("org.bouncycastle:bcpg-fips", ExcludeAllTransitivesRule.class);
        components.withModule("org.bouncycastle:bcpkix-jdk18on", ExcludeAllTransitivesRule.class);
        components.withModule("org.bouncycastle:bcutil-jdk18on", ExcludeAllTransitivesRule.class);

        // Carrot2 dependencies
        components.withModule("org.carrot2:morfologik-stemming", ExcludeAllTransitivesRule.class);

        // Kotlin dependencies
        components.withModule("org.jetbrains.kotlin:kotlin-stdlib", ExcludeAllTransitivesRule.class);

        // JONI dependencies
        components.withModule("org.jruby.joni:joni", ExcludeAllTransitivesRule.class);

        // JUnit dependencies
        components.withModule("org.junit.jupiter:junit-jupiter", ExcludeAllTransitivesRule.class);

        // Mockito dependencies
        components.withModule("org.mockito:mockito-core", ExcludeAllTransitivesRule.class);
        components.withModule("org.mockito:mockito-subclass", ExcludeAllTransitivesRule.class);

        // JMH dependencies
        components.withModule("org.openjdk.jmh:jmh-core", ExcludeAllTransitivesRule.class);

        // OpenSAML dependencies
        components.withModule("org.opensaml:opensaml-core", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-messaging-api", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-messaging-impl", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-profile-api", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-profile-impl", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-saml-api", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-saml-impl", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-security-api", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-security-impl", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-soap-api", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-soap-impl", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-storage-api", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-storage-impl", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-xmlsec-api", ExcludeAllTransitivesRule.class);
        components.withModule("org.opensaml:opensaml-xmlsec-impl", ExcludeAllTransitivesRule.class);

        // OrbisGIS dependencies
        components.withModule("org.orbisgis:cts", ExcludeAllTransitivesRule.class);
        components.withModule("org.orbisgis:h2gis", ExcludeAllTransitivesRule.class);
        components.withModule("org.orbisgis:h2gis-utilities", ExcludeAllTransitivesRule.class);

        // ASM dependencies
        components.withModule("org.ow2.asm:asm-analysis", ExcludeAllTransitivesRule.class);
        components.withModule("org.ow2.asm:asm-commons", ExcludeAllTransitivesRule.class);
        components.withModule("org.ow2.asm:asm-tree", ExcludeAllTransitivesRule.class);
        components.withModule("org.ow2.asm:asm-util", ExcludeAllTransitivesRule.class);

        // Reactive Streams dependencies
        components.withModule("org.reactivestreams:reactive-streams-tck", ExcludeAllTransitivesRule.class);

        // SLF4J dependencies
        components.withModule("org.slf4j:jcl-over-slf4j", ExcludeAllTransitivesRule.class);
        components.withModule("org.slf4j:slf4j-nop", ExcludeAllTransitivesRule.class);
        components.withModule("org.slf4j:slf4j-simple", ExcludeAllTransitivesRule.class);

        // SubEtha SMTP dependencies
        components.withModule("org.subethamail:subethasmtp", ExcludeAllTransitivesRule.class);

        // AWS SDK dependencies
        components.withModule("software.amazon.awssdk:apache-client", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:arns", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:auth", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:aws-core", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:aws-json-protocol", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:aws-query-protocol", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:aws-xml-protocol", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:checksums", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:checksums-spi", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:ec2", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:endpoints-spi", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:http-auth", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:http-auth-aws", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:http-auth-aws-eventstream", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:http-auth-spi", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:http-client-spi", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:identity-spi", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:imds", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:json-utils", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:metrics-spi", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:netty-nio-client", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:profiles", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:protocol-core", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:regions", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:retries", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:retries-spi", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:s3", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:sdk-core", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:services", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:sts", ExcludeAllTransitivesRule.class);
        components.withModule("software.amazon.awssdk:utils", ExcludeAllTransitivesRule.class);
    }
}
