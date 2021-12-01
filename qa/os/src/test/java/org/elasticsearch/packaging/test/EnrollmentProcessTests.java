/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.packaging.util.Archives;
import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Shell;
import org.elasticsearch.xpack.core.security.EnrollmentToken;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
            Archives.startElasticsearchWithTty(installation, sh, null, List.of(), false)
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

        // Try to start the node with an invalid enrollment token and verify it fails to start
        Shell.Result startSecondNodeWithInvalidToken = Archives.startElasticsearchWithTty(
            installation,
            sh,
            null,
            List.of("--enrollment-token", "some-invalid-token-here"),
            false
        );
        assertThat(
            startSecondNodeWithInvalidToken.stdout,
            containsString("Failed to parse enrollment token : some-invalid-token-here . Error was: Illegal base64 character 2d")
        );
        verifySecurityNotAutoConfigured(installation);

        // Try to start the node with an enrollment token with an address that we can't connect to and verify it fails to start
        EnrollmentToken tokenWithWrongAddress = new EnrollmentToken(
            "some-api-key",
            "some-fingerprint",
            Version.CURRENT.toString(),
            List.of("10.1.3.4:9200")
        );
        // TODO Wait for a set amount of time for stdout to contain the actual error and then check the output. If we try to
        // use an enrollment token with an invalid address, it will eventually fail and print the appropriate error message, but we would
        // have read the result (and stdout) from Archives#startElasticsearchWithTty earlier than that
        Archives.startElasticsearchWithTty(
            installation,
            sh,
            null,
            List.of("--enrollment-token", tokenWithWrongAddress.getEncoded()),
            false

        );
        // the autoconfiguration dir will be cleaned _after_ we fail to connect to the supposed original node. Allow time for this to happen
        assertBusy(() -> assertThat(getAutoConfigDirName(installation).isPresent(), Matchers.is(false)), 20, TimeUnit.SECONDS);
        // auto-configure security using the enrollment token
        Shell.Result startSecondNode = awaitElasticsearchStartupWithResult(
            Archives.startElasticsearchWithTty(installation, sh, null, List.of("--enrollment-token", enrollmentToken), false)
        );
        // ugly hack, wait for the second node to actually start and join the cluster, all of our current tooling expects/assumes
        // a single installation listening on 9200
        // TODO Make our packaging test methods aware of multiple installations, see https://github.com/elastic/elasticsearch/issues/79688
        waitForSecondNode();
        assertThat(startSecondNode.isSuccess(), is(true));
        verifySecurityAutoConfigured(installation);
        // verify that the two nodes formed a cluster
        assertThat(makeRequest("https://localhost:9200/_cluster/health"), containsString("\"number_of_nodes\":2"));
    }

    private void waitForSecondNode() {
        int retries = 60;
        while (retries > 0) {
            retries -= 1;
            try (Socket s = new Socket(InetAddress.getLoopbackAddress(), 9201)) {
                return;
            } catch (IOException e) {
                // ignore, only want to establish a connection
            }

            try {
                Thread.sleep(2000);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        throw new RuntimeException("Elasticsearch second node did not start listening on 9201");
    }
}
