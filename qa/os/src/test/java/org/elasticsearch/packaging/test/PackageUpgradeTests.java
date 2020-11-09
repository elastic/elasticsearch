/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        verifyPackageInstallation(installation, bwcDistribution, sh);
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
            Packages.forceUpgradePackage(sh, distribution);
        } else {
            Packages.upgradePackage(sh, distribution);
        }
        assertInstalled(distribution);
        verifyPackageInstallation(installation, distribution, sh);
    }

    public void test21CheckUpgradedVersion() throws Exception {
        assertWhileRunning(() -> { assertDocsExist(); });
    }

    private void assertDocsExist() throws Exception {
        String response1 = makeRequest(Request.Get("http://localhost:9200/library/_doc/1?pretty"));
        assertThat(response1, containsString("Elasticsearch"));
        String response2 = makeRequest(Request.Get("http://localhost:9200/library/_doc/2?pretty"));
        assertThat(response2, containsString("World"));
        String response3 = makeRequest(Request.Get("http://localhost:9200/library2/_doc/1?pretty"));
        assertThat(response3, containsString("Darkness"));
    }
}
