/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import javax.net.ssl.X509ExtendedTrustManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class RestrictedTrustConfigTests extends ESTestCase {

    public void testDelegationOfFilesToMonitor() throws Exception {
        Path homeDir = createTempDir();
        Settings settings = Settings.builder().put("path.home", homeDir).build();
        Environment environment = new Environment(settings);

        final int numOtherFiles = randomIntBetween(0, 4);
        List<Path> otherFiles = new ArrayList<>(numOtherFiles);
        for (int i = 0; i < numOtherFiles; i++) {
            otherFiles.add(Files.createFile(homeDir.resolve("otherFile" + i)));
        }
        Path groupConfigPath = Files.createFile(homeDir.resolve("groupConfig"));

        TrustConfig delegate = new TrustConfig() {
            @Override
            X509ExtendedTrustManager createTrustManager(Environment environment) {
                return null;
            }

            @Override
            List<Path> filesToMonitor(Environment environment) {
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

        final RestrictedTrustConfig restrictedTrustConfig = new RestrictedTrustConfig(settings, groupConfigPath.toString(), delegate);
        List<Path> filesToMonitor = restrictedTrustConfig.filesToMonitor(environment);
        List<Path> expectedPathList = new ArrayList<>(otherFiles);
        expectedPathList.add(groupConfigPath);

        assertEquals(numOtherFiles + 1, filesToMonitor.size());
        assertThat(filesToMonitor, Matchers.contains(expectedPathList.toArray(new Path[0])));
    }
}
