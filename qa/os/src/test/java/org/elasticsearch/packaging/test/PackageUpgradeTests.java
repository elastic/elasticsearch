/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.elasticsearch.Version;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Packages;
import org.elasticsearch.packaging.util.ServerUtils;
import org.junit.BeforeClass;

import java.nio.file.Paths;

import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.installPackage;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assume.assumeTrue;

public class PackageUpgradeTests extends PackagingTestCase {

    // the distribution being upgraded
    protected static final Distribution bwcDistribution;
    static {
        bwcDistribution = new Distribution(Paths.get(System.getProperty("tests.bwc-distribution")));
    }

    @BeforeClass
    public static void filterVersions() {
        // TODO: Explicitly add testing for these versions that validates that starting the node after upgrade fails
        assumeTrue("only wire compatible versions", Version.fromString(bwcDistribution.baseVersion).isCompatible(Version.CURRENT));
    }

    public void test10InstallBwcVersion() throws Exception {
        installation = installPackage(sh, bwcDistribution);
        assertInstalled(bwcDistribution);
        // TODO: Add more tests here to assert behavior when updating from < v8 to > v8 with implicit/explicit behavior,
        // maybe as part of https://github.com/elastic/elasticsearch/pull/76879
        ServerUtils.disableSecurityFeatures(installation);
    }

    public void test11ModifyKeystore() throws Exception {
        // deliberately modify the keystore to force it to be preserved during package upgrade
        installation.executables().keystoreTool.run("remove keystore.seed");
        installation.executables().keystoreTool.run("add -x keystore.seed", "keystore_seed");
    }

    public void test12SetupBwcVersion() throws Exception {
        startElasticsearch();

        // create indexes explicitly with 0 replicas so when restarting we can reach green state
        ServerUtils.makeRequest(
            Request.Put("http://localhost:9200/library")
                .bodyString("{\"settings\":{\"index\":{\"number_of_replicas\":0}}}", ContentType.APPLICATION_JSON)
        );
        ServerUtils.makeRequest(
            Request.Put("http://localhost:9200/library2")
                .bodyString("{\"settings\":{\"index\":{\"number_of_replicas\":0}}}", ContentType.APPLICATION_JSON)
        );

        // add some docs
        ServerUtils.makeRequest(
            Request.Post("http://localhost:9200/library/_doc/1?refresh=true&pretty")
                .bodyString("{ \"title\": \"Elasticsearch - The Definitive Guide\"}", ContentType.APPLICATION_JSON)
        );
        ServerUtils.makeRequest(
            Request.Post("http://localhost:9200/library/_doc/2?refresh=true&pretty")
                .bodyString("{ \"title\": \"Brave New World\"}", ContentType.APPLICATION_JSON)
        );
        ServerUtils.makeRequest(
            Request.Post("http://localhost:9200/library2/_doc/1?refresh=true&pretty")
                .bodyString("{ \"title\": \"The Left Hand of Darkness\"}", ContentType.APPLICATION_JSON)
        );

        assertDocsExist();

        stopElasticsearch();
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/79950")
    public void test20InstallUpgradedVersion() throws Exception {
        if (bwcDistribution.path.equals(distribution.path)) {
            // the old and new distributions are the same, so we are testing force upgrading
            installation = Packages.forceUpgradePackage(sh, distribution);
        } else {
            installation = Packages.upgradePackage(sh, distribution);
            verifySecurityNotAutoConfigured(installation);
        }
        assertInstalled(distribution);
        verifyPackageInstallation(installation, distribution, sh);
        // Upgrade overwrites the configuration file because we run with --force-confnew so we need to disable security again
        ServerUtils.disableSecurityFeatures(installation);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/76283")
    public void test21CheckUpgradedVersion() throws Exception {
        assertWhileRunning(() -> { assertDocsExist(); });
    }

    private void assertDocsExist() throws Exception {
        String response1 = ServerUtils.makeRequest(Request.Get("http://localhost:9200/library/_doc/1?pretty"));
        assertThat(response1, containsString("Elasticsearch"));
        String response2 = ServerUtils.makeRequest(Request.Get("http://localhost:9200/library/_doc/2?pretty"));
        assertThat(response2, containsString("World"));
        String response3 = ServerUtils.makeRequest(Request.Get("http://localhost:9200/library2/_doc/1?pretty"));
        assertThat(response3, containsString("Darkness"));
    }
}
