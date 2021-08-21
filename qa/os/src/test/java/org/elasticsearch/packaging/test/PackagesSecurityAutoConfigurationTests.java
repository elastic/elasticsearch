/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Installation;
import org.elasticsearch.packaging.util.Packages;
import org.elasticsearch.packaging.util.Shell;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static org.elasticsearch.packaging.util.FileMatcher.Fileness.Directory;
import static org.elasticsearch.packaging.util.FileMatcher.Fileness.File;
import static org.elasticsearch.packaging.util.FileMatcher.file;
import static org.elasticsearch.packaging.util.FileMatcher.p660;
import static org.elasticsearch.packaging.util.FileMatcher.p755;
import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.assertRemoved;
import static org.elasticsearch.packaging.util.Packages.installPackage;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeTrue;

public class PackagesSecurityAutoConfigurationTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue(
            "only deb and rpm",
            distribution.packaging == Distribution.Packaging.DEB || distribution.packaging == Distribution.Packaging.RPM
        );
    }

    public void test10SecurityAutoConfiguredOnPackageInstall() throws Exception {
        assertRemoved(distribution());
        installation = installPackage(sh, distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
        verifySecurityAutoConfigured(installation);
        assertNotNull(installation.getElasticPassword());
    }

    public void test20SecurityNotAutoConfiguredOnPackageUpgrade() throws Exception {
        final Distribution bwcDistribution = new Distribution(Paths.get(System.getProperty("tests.bwc-distribution")));
        installation = installPackage(sh, bwcDistribution);
        assertInstalled(bwcDistribution);
        if (bwcDistribution.path.equals(distribution.path)) {
            // the old and new distributions are the same, so we are testing force upgrading
            installation = Packages.forceUpgradePackage(sh, distribution);
        } else {
            installation = Packages.upgradePackage(sh, distribution);
        }
        assertInstalled(distribution);
        verifyPackageInstallation(installation, distribution, sh);
        verifySecurityNotAutoConfigured(installation);
    }

    private static void verifySecurityNotAutoConfigured(Installation es) throws Exception {
        final Shell.Result lsResult = sh.run("ls", es.config);
        assertNotNull(lsResult.stdout);
        assertThat((int) Arrays.stream(lsResult.stdout.split(" ")).filter(f -> f.startsWith("auto_config_on")).count(), equalTo(0));
        assertThat(sh.run(es.bin("elasticsearch-keystore") + " list").stdout, not(containsString("autoconfig.password_hash")));
        List<String> configLines = Files.readAllLines(es.config("elasticsearch.yml"));
        assertThat(configLines, not(contains("# have been automatically generated in order to configure Security.               #")));
    }

    private static void verifySecurityAutoConfigured(Installation es) throws IOException {
        final Shell.Result lsResult = sh.run("ls", es.config);
        assertNotNull(lsResult.stdout);
        Optional<String> autoConfigDirName = Arrays.stream(lsResult.stdout.split(" "))
            .filter(f -> f.startsWith("auto_config_on"))
            .findFirst();
        assertThat(autoConfigDirName.isPresent(), is(true));
        assertThat(es.config(autoConfigDirName.get()), file(Directory, "root", "elasticsearch", p755));
        Stream.of("http_keystore_local_node.p12", "http_ca.crt", "transport_keystore_all_nodes.p12")
            .forEach(file -> assertThat(es.config(autoConfigDirName.get()).resolve(file), file(File, "root", "elasticsearch", p660)));
        List<String> configLines = Files.readAllLines(es.config("elasticsearch.yml"));
        assertThat(configLines, contains("xpack.security.enabled: true"));
        assertThat(configLines, contains("xpack.security.enrollment.enabled: true"));
        assertThat(configLines, contains("xpack.security.transport.ssl.enabled: true"));
        assertThat(configLines, contains("xpack.security.transport.ssl.verification_mode: certificate"));
        assertThat(
            configLines,
            contains(
                "xpack.security.transport.ssl.keystore.path: "
                    + es.config(autoConfigDirName.get()).resolve("transport_keystore_all_nodes.p12")
            )
        );
        assertThat(
            configLines,
            contains(
                "xpack.security.transport.ssl.truststore.path: "
                    + es.config(autoConfigDirName.get()).resolve("transport_keystore_all_nodes.p12")
            )
        );
        assertThat(configLines, contains("xpack.security.http.ssl.enabled: true"));
        assertThat(
            configLines,
            contains("xpack.security.http.ssl.keystore.path: " + es.config(autoConfigDirName.get()).resolve("http_keystore_local_node.p12"))
        );
        assertThat(
            configLines,
            contains("xpack.security.http.ssl.truststore.path: " + "/etc/elasticsearch/auto_generated_certs/http_truststore.p12")
        );
        assertThat(configLines, contains("http.host: [_local_, _site_]"));
        assertThat(sh.run(es.bin("elasticsearch-keystore") + " list").stdout, containsString("autoconfig.password_hash"));
    }

}
