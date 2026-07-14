/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterSpecBuilder;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.rules.TemporaryFolder;

import java.util.function.Supplier;

/**
 * The x-pack rolling upgrade cluster configuration shared by {@link AbstractXPackRollingUpgradeTestCase} and
 * {@link AbstractXPackYamlRollingUpgradeTestCase}.
 *
 * <p>These two base classes cannot share a common ancestor: one extends {@code ParameterizedRollingUpgradeTestCase}
 * (a plain {@code ESRestTestCase}) and the other extends {@code ESClientYamlSuiteTestCase}, and both of those
 * already own the single {@code @ParametersFactory} method a test class is allowed to have. Rather than duplicate
 * this large settings block in both classes, it lives here as a shared static helper.
 */
final class XPackRollingUpgradeClusterConfig {

    static final int NODE_NUM = 3;

    private XPackRollingUpgradeClusterConfig() {}

    static ElasticsearchCluster buildCluster(String oldClusterVersion, boolean oldClusterDetachedVersion, TemporaryFolder repoDirectory) {
        LocalClusterSpecBuilder<ElasticsearchCluster> cluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .version(oldClusterVersion, oldClusterDetachedVersion)
            .nodes(NODE_NUM)
            .setting("xpack.license.self_generated.type", "trial")
            .setting("xpack.security.enabled", "true")
            .setting("xpack.security.transport.ssl.enabled", "true")
            .setting("xpack.security.transport.ssl.key", "testnode.pem")
            .setting("xpack.security.transport.ssl.certificate", "testnode.crt")
            .keystore("xpack.security.transport.ssl.secure_key_passphrase", "testnode")
            .setting("xpack.security.authc.token.enabled", "true")
            .setting("xpack.security.authc.token.timeout", "60m")
            .setting("xpack.security.authc.api_key.enabled", "true")
            .setting("xpack.security.audit.enabled", "true")
            .setting("xpack.watcher.encrypt_sensitive_data", "true")
            .setting("logger.org.elasticsearch.xpack.watcher", "DEBUG")
            .setting("ingest.geoip.downloader.enabled.default", "true")
            .setting("repositories.url.allowed_urls", "http://snapshot.test*")
            .configFile("testnode.pem", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .configFile("testnode.crt", Resource.fromClasspath("org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .keystore("xpack.watcher.encryption_key", Resource.fromClasspath("system_key"))
            .user("test_user", "x-pack-test-password")
            .setting("path.repo", new Supplier<>() {
                @Override
                @SuppressForbidden(reason = "TemporaryFolder only has io.File methods, not nio.File")
                public String get() {
                    return repoDirectory.getRoot().getPath();
                }
            });

        Version oldVersion = Version.tryParse(oldClusterVersion).orElse(null);

        if (oldVersion != null && oldVersion.onOrAfter(Version.fromString("7.0.0"))) {
            cluster.setting("xpack.security.authc.realms.file.file1.order", "0");
            cluster.setting("xpack.security.authc.realms.native.native1.order", "1");
        } else if (oldVersion != null) {
            cluster.setting("xpack.security.authc.realms.file1.type", "file");
            cluster.setting("xpack.security.authc.realms.file1.order", "0");
            cluster.setting("xpack.security.authc.realms.native1.type", "native");
            cluster.setting("xpack.security.authc.realms.native1.order", "1");
        } else {
            cluster.setting("xpack.security.authc.realms.file.file1.order", "0");
            cluster.setting("xpack.security.authc.realms.native.native1.order", "1");
        }

        if (oldVersion == null || oldVersion.onOrAfter(Version.fromString("6.6.0"))) {
            cluster.setting("ccr.auto_follow.wait_for_metadata_timeout", "1s");
        }

        if (oldVersion == null || oldVersion.onOrAfter(Version.fromString("7.11.0"))) {
            cluster.configFile("operator_users.yml", Resource.fromClasspath("operator_users.yml"));
            cluster.setting("xpack.security.operator_privileges.enabled", "true");
            cluster.user("non_operator", "x-pack-test-password", "superuser", false);
        }

        if (oldVersion == null || oldVersion.onOrAfter(Version.fromString("8.7.0"))) {
            cluster.configFile("operator/settings.json", Resource.fromClasspath("operator_defined_role_mappings.json"));
        }

        if (oldVersion == null || oldVersion.onOrAfter(Version.fromString("7.14.0"))) {
            cluster.setting("ingest.geoip.downloader.endpoint", "http://invalid.endpoint");
        }

        if (oldVersion == null || oldVersion.onOrAfter(Version.fromString("7.12.0"))) {
            cluster.setting("xpack.searchable.snapshot.shared_cache.size", "16MB");
            cluster.setting("xpack.searchable.snapshot.shared_cache.region_size", "256KB");
        }

        // Avoid triggering bogus assertion when serialized parsed mappings don't match with original mappings, because _source key is
        // inconsistent. As usual, we operate under the premise that "versionless" clusters (serverless) are on the latest code and
        // do not need this.
        if (oldVersion != null && oldVersion.before(Version.fromString("8.18.0"))) {
            cluster.jvmArg("-da:org.elasticsearch.index.mapper.DocumentMapper");
            cluster.jvmArg("-da:org.elasticsearch.index.mapper.MapperService");
        }

        return cluster.build();
    }
}
