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
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Packages;

import java.nio.file.Paths;

import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.installPackage;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.elasticsearch.packaging.util.ServerUtils.makeRequest;
import static org.hamcrest.Matchers.containsString;

public class PackageUpgradeTests extends PackagingTestCase {

    // the distribution being upgraded
    protected static final Distribution bwcDistribution;
    static {
        bwcDistribution = new Distribution(Paths.get(System.getProperty("tests.bwc-distribution")));
    }

    public void test10InstallBwcVersion() throws Exception {
        installation = installPackage(sh, bwcDistribution);
        assertInstalled(bwcDistribution);
    }

    public void test11ModifyKeystore() throws Exception {
        // deliberately modify the keystore to force it to be preserved during package upgrade
        installation.executables().keystoreTool.run("remove keystore.seed");
        installation.executables().keystoreTool.run("add -x keystore.seed", "keystore_seed");
    }

    public void test12SetupBwcVersion() throws Exception {
        startElasticsearch();

        // create indexes explicitly with 0 replicas so when restarting we can reach green state
        makeRequest(
            Request.Put("http://localhost:9200/library")
                .bodyString("{\"settings\":{\"index\":{\"number_of_replicas\":0}}}", ContentType.APPLICATION_JSON)
        );
        makeRequest(
            Request.Put("http://localhost:9200/library2")
                .bodyString("{\"settings\":{\"index\":{\"number_of_replicas\":0}}}", ContentType.APPLICATION_JSON)
        );

        // add some docs
        makeRequest(
            Request.Post("http://localhost:9200/library/_doc/1?refresh=true&pretty")
                .bodyString("{ \"title\": \"Elasticsearch - The Definitive Guide\"}", ContentType.APPLICATION_JSON)
        );
        makeRequest(
            Request.Post("http://localhost:9200/library/_doc/2?refresh=true&pretty")
                .bodyString("{ \"title\": \"Brave New World\"}", ContentType.APPLICATION_JSON)
        );
        makeRequest(
            Request.Post("http://localhost:9200/library2/_doc/1?refresh=true&pretty")
                .bodyString("{ \"title\": \"The Left Hand of Darkness\"}", ContentType.APPLICATION_JSON)
        );

        assertDocsExist();

        stopElasticsearch();
    }

    public void test20InstallUpgradedVersion() throws Exception {
        if (bwcDistribution.path.equals(distribution.path)) {
            // the old and new distributions are the same, so we are testing force upgrading
            installation = Packages.forceUpgradePackage(sh, distribution);
        } else {
            installation = Packages.upgradePackage(sh, distribution);
        }
        assertInstalled(distribution);
        verifyPackageInstallation(installation, distribution, sh);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/76283")
    public void test21CheckUpgradedVersion() throws Exception {
        assertWhileRunning(() -> { assertDocsExist(); });
    }

    private void assertDocsExist() throws Exception {
        // We can properly handle this as part of https://github.com/elastic/elasticsearch/issues/75940
        // For now we can use elastic with "keystore.seed" as we set it explicitly in PackageUpgradeTests#test11ModifyKeystore
        String response1 = makeRequest(Request.Get("http://localhost:9200/library/_doc/1?pretty"), "elastic", "keystore_seed", null);
        assertThat(response1, containsString("Elasticsearch"));
        String response2 = makeRequest(Request.Get("http://localhost:9200/library/_doc/2?pretty"), "elastic", "keystore_seed", null);
        assertThat(response2, containsString("World"));
        String response3 = makeRequest(Request.Get("http://localhost:9200/library2/_doc/1?pretty"), "elastic", "keystore_seed", null);
        assertThat(response3, containsString("Darkness"));
    }
}
