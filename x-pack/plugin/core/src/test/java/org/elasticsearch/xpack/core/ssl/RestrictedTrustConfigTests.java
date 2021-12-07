/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslTrustConfig;
import org.elasticsearch.common.ssl.StoredCertificate;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.net.ssl.X509ExtendedTrustManager;

public class RestrictedTrustConfigTests extends ESTestCase {

    public void testDelegationOfFilesToMonitor() throws Exception {
        Path homeDir = createTempDir();
        Settings settings = Settings.builder().put("path.home", homeDir).build();
        Environment environment = TestEnvironment.newEnvironment(settings);

        final int numOtherFiles = randomIntBetween(0, 4);
        List<Path> otherFiles = new ArrayList<>(numOtherFiles);
        for (int i = 0; i < numOtherFiles; i++) {
            otherFiles.add(Files.createFile(homeDir.resolve("otherFile" + i)));
        }
        Path groupConfigPath = Files.createFile(homeDir.resolve("groupConfig"));

        SslTrustConfig delegate = new SslTrustConfig() {
            @Override
            public X509ExtendedTrustManager createTrustManager() {
                return null;
            }

            @Override
            public Collection<? extends StoredCertificate> getConfiguredCertificates() {
                return List.of();
            }

            @Override
            public Collection<Path> getDependentFiles() {
                return otherFiles;
            }

            @Override
            public String toString() {
                return null;
            }

            @Override
            public boolean equals(Object o) {
                return false;
            }

            @Override
            public int hashCode() {
                return 0;
            }
        };

        final RestrictedTrustConfig restrictedTrustConfig = new RestrictedTrustConfig(groupConfigPath, delegate);
        Collection<Path> filesToMonitor = restrictedTrustConfig.getDependentFiles();
        List<Path> expectedPathList = new ArrayList<>(otherFiles);
        expectedPathList.add(groupConfigPath);

        assertEquals(numOtherFiles + 1, filesToMonitor.size());
        assertThat(filesToMonitor, Matchers.contains(expectedPathList.toArray(new Path[0])));
    }
}
