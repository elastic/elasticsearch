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
import org.elasticsearch.packaging.util.Shell;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.packaging.util.Archives.installArchive;
import static org.elasticsearch.packaging.util.Archives.verifyArchiveInstallation;
import static org.elasticsearch.packaging.util.FileUtils.getCurrentVersion;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class EnrollmentProcessTests extends PackagingTestCase {
    private static final Pattern PASSWORD_REGEX = Pattern.compile("Password for the elastic user is: (.+)$", Pattern.MULTILINE);

    public void test10AutoFormCluster() throws Exception {
        installation = installArchive(
            sh,
            distribution(),
            getRootTempDir().resolve("elasticsearch-node1"),
            getCurrentVersion(),
            true
        );
        verifyArchiveInstallation(installation, distribution());
        sh.getEnv().put("ES_JAVA_OPTS", "-Xms1g -Xmx1g");
        Shell.Result startFirstNode = awaitElasticsearchStartupWithResult(
            Archives.startElasticsearchWithTty(installation, sh, null, false)
        );
        // Capture auto-generated password of the elastic user from the node startup output
        final String elasticPassword = parseElasticPassword(startFirstNode.stdout);
        assertNotNull(elasticPassword);
        // Verify that the first node was auto-configured for security
        verifySecurityAutoConfigured(installation);
        // Generate a node enrollment token to be subsequently used by the second node
        Shell.Result createTokenResult = installation.executables().createEnrollmentToken.run("-s node");
        assertThat(Strings.isNullOrEmpty(createTokenResult.stdout), is(false));
        final String enrollmentToken = createTokenResult.stdout;
        // installation now points to the second node
        installation = installArchive(
            sh,
            distribution(),
            getRootTempDir().resolve("elasticsearch-node2"),
            getCurrentVersion(),
            true
        );
        // auto-configure security using the enrollment token
        installation.executables().enrollToExistingCluster.run("--enrollment-token " + enrollmentToken);
        verifySecurityAutoConfigured(installation);
        Shell.Result startSecondNode = awaitElasticsearchStartupWithResult(
            Archives.startElasticsearchWithTty(installation, sh, null, false)
        );
        assertThat(startSecondNode.exitCode, is(0));
        assertNull(parseElasticPassword(startSecondNode.stdout));
        // verify that the two nodes formed a cluster
        assertThat(
            makeRequestAsElastic("https://localhost:9200/_cluster/health", elasticPassword),
            containsString("\"number_of_nodes\":2")
        );
    }

    private String parseElasticPassword(String output) {
        Matcher matcher = PASSWORD_REGEX.matcher(output);
        assertNotNull(matcher);
        if (matcher.find()) {
            return matcher.group(1);
        } else {
            return null;
        }
    }
}
