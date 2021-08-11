/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.packaging.test;

import org.elasticsearch.packaging.util.Shell;
import org.junit.BeforeClass;

import static org.elasticsearch.packaging.util.Packages.assertInstalled;
import static org.elasticsearch.packaging.util.Packages.assertRemoved;
import static org.elasticsearch.packaging.util.Packages.installPackage;
import static org.elasticsearch.packaging.util.Packages.verifyPackageInstallation;
import static org.elasticsearch.packaging.util.ServerUtils.validateCredentials;
import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assume.assumeTrue;

public class PackageSecurityAutoconfigurationTests extends PackagingTestCase {

    @BeforeClass
    public static void filterDistros() {
        assumeTrue("rpm or deb", distribution.isPackage());
    }

    public void test10ElasticPasswordHash() throws Exception {
        assertRemoved(distribution());
        installation = installPackage(sh, distribution());
        assertInstalled(distribution());
        verifyPackageInstallation(installation, distribution(), sh);
        Shell.Result keystoreListResult = installation.executables().keystoreTool.run("list");
        // Keystore should be created already by the installation and it should contain only "keystore.seed" at this point
        assertThat(keystoreListResult.stdout, containsString("keystore.seed"));
        // With future changes merged, this would be automatically populated on installation. For now, add it manually
        installation.executables().keystoreTool.
        // $2a$10$R2oFwbHR/9x9.e/bQpJ6IeHKUVP08KHQ9LcZPMlWeyuQuYboR82fm is the hash of thisisalongenoughpassword
            run("add -x autoconfiguration.password_hash", "$2a$10$R2oFwbHR/9x9.e/bQpJ6IeHKUVP08KHQ9LcZPMlWeyuQuYboR82fm");
        startElasticsearch();
        validateCredentials("elastic", "thisisalongenoughpassword", null);
    }
}
