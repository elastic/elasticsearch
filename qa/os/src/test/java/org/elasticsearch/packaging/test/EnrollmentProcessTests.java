/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.common.Strings;
import org.elasticsearch.packaging.util.Archives;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Shell;
import org.junit.BeforeClass;

import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.elasticsearch.packaging.util.FileUtils.getCurrentVersion;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assume.assumeTrue;

public class EnrollmentProcessTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only archives", distribution.isArchive());
    }

    public void test10AutoFormCluster() throws Exception {
        /* Windows issue awaits fix: https://github.com/elastic/elasticsearch/issues/49340 */
        assumeTrue("expect command isn't on Windows", distribution.platform != Distribution.Platform.WINDOWS);
        installation = installArchive(sh, distribution(), getRootTempDir().resolve("elasticsearch-node1"), getCurrentVersion(), true);
        verifyArchiveInstallation(installation, distribution());
        setFileSuperuser("test_superuser", "test_superuser_password");
        sh.getEnv().put("ES_JAVA_OPTS", "-Xms1g -Xmx1g");
        Shell.Result startFirstNode = awaitElasticsearchStartupWithResult(
            Archives.startElasticsearchWithTty(installation, sh, null, false)
        );
        assertThat(startFirstNode.isSuccess(), is(true));
        // Verify that the first node was auto-configured for security
        verifySecurityAutoConfigured(installation);
        // Generate a node enrollment token to be subsequently used by the second node
        Shell.Result createTokenResult = installation.executables().createEnrollmentToken.run("-s node");
        assertThat(Strings.isNullOrEmpty(createTokenResult.stdout), is(false));
        final String enrollmentToken = createTokenResult.stdout;
        // installation now points to the second node
        installation = installArchive(sh, distribution(), getRootTempDir().resolve("elasticsearch-node2"), getCurrentVersion(), true);
        // auto-configure security using the enrollment token
        installation.executables().enrollToExistingCluster.run("--enrollment-token " + enrollmentToken);
        // Verify that the second node was also configured (via enrollment) for security
        verifySecurityAutoConfigured(installation);
        Shell.Result startSecondNode = awaitElasticsearchStartupWithResult(
            Archives.startElasticsearchWithTty(installation, sh, null, false)
        );
        assertThat(startSecondNode.isSuccess(), is(true));
        // verify that the two nodes formed a cluster
        assertThat(makeRequest("https://localhost:9200/_cluster/health"), containsString("\"number_of_nodes\":2"));
    }
}
