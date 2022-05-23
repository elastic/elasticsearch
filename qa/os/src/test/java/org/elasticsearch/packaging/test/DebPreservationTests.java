/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Distribution;
import org.elasticsearch.packaging.util.Packages;
import org.elasticsearch.packaging.util.ServerUtils;
import org.junit.BeforeClass;

import java.nio.file.Paths;

import static org.elasticsearch.packaging.util.FileExistenceMatchers.fileExists;
import static org.elasticsearch.packaging.util.FileUtils.append;
import static org.elasticsearch.packaging.util.FileUtils.assertPathsDoNotExist;
import static org.elasticsearch.packaging.util.FileUtils.assertPathsExist;
import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.assertRemoved;
import static org.elasticsearch.packaging.util.Packages.installPackage;
import static org.elasticsearch.packaging.util.Packages.packageStatus;
import static org.elasticsearch.packaging.util.Packages.remove;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.hamcrest.core.Is.is;
import static org.junit.Assume.assumeTrue;

public class DebPreservationTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("only deb", distribution.packaging == Distribution.Packaging.DEB);
        assumeTrue("only bundled jdk", distribution.hasJdk);
    }

    public void test10Install() throws Exception {
        assertRemoved(distribution());
        installation = installPackage(sh, distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
    }

    public void test20Remove() throws Exception {
        append(installation.config(Paths.get("jvm.options.d", "heap.options")), "# foo");

        remove(distribution());

        // some config files were not removed
        assertPathsExist(
            installation.config,
            installation.config("elasticsearch.yml"),
            installation.config("jvm.options"),
            installation.config("log4j2.properties"),
            installation.config(Paths.get("jvm.options.d", "heap.options"))
        );

        assertPathsExist(
            installation.config,
            installation.config("role_mapping.yml"),
            installation.config("roles.yml"),
            installation.config("users"),
            installation.config("users_roles")
        );

        // keystore was not removed
        assertPathsExist(installation.config("elasticsearch.keystore"));

        // doc files were removed
        assertPathsDoNotExist(Paths.get("/usr/share/doc/elasticsearch"), Paths.get("/usr/share/doc/elasticsearch/copyright"));

        // defaults file was not removed
        assertThat(installation.envFile, fileExists());
    }

    public void test30Purge() throws Exception {
        append(installation.config(Paths.get("jvm.options.d", "heap.options")), "# foo");

        sh.run("dpkg --purge elasticsearch");

        assertRemoved(distribution());

        assertPathsDoNotExist(installation.config, installation.envFile);

        assertThat(packageStatus(distribution()).exitCode(), is(1));

        installation = null;
    }

    /**
     * Check that restarting on upgrade doesn't run into a problem where the keystore
     * upgrade is attempted as the wrong user i.e. the restart happens at the correct
     * point. See #82433.
     */
    public void test40RestartOnUpgrade() throws Exception {
        assertRemoved(distribution());
        installation = installPackage(sh, distribution());
        assertInstalled(distribution());

        // Ensure ES is started
        Packages.runElasticsearchStartCommand(sh);
        ServerUtils.waitForElasticsearch(installation);

        sh.getEnv().put("RESTART_ON_UPGRADE", "true");
        installation = installPackage(sh, distribution());

        ServerUtils.waitForElasticsearch(installation);
    }
}
