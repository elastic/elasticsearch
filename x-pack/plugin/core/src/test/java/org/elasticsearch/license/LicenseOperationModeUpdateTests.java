/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.junit.Before;

import java.nio.file.Path;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class LicenseOperationModeUpdateTests extends ESTestCase {

    private OperationModeFileWatcher operationModeFileWatcher;
    private Path licenseModeFile;
    private ResourceWatcherService resourceWatcherService;

    @Before
    public void init() throws Exception {
        licenseModeFile = createTempFile();
        resourceWatcherService = mock(ResourceWatcherService.class);
        operationModeFileWatcher = new OperationModeFileWatcher(resourceWatcherService, licenseModeFile, logger, () -> {});
    }

    public void testLicenseOperationModeUpdate() throws Exception {
        String type = randomFrom("trial", "basic", "standard", "gold", "platinum");
        License license = License.builder()
                .uid("id")
                .expiryDate(0)
                .issueDate(0)
                .issuedTo("elasticsearch")
                .issuer("issuer")
                .type(type)
                .maxNodes(1)
                .build();

        assertThat(license.operationMode(), equalTo(License.OperationMode.resolve(type)));
        OperationModeFileWatcherTests.writeMode("gold", licenseModeFile);
        license.setOperationModeFileWatcher(operationModeFileWatcher);
        verifyZeroInteractions(resourceWatcherService);
        assertThat(license.operationMode(), equalTo(License.OperationMode.resolve(type)));
    }

    public void testCloudInternalLicenseOperationModeUpdate() throws Exception {
        License license = License.builder()
                .uid("id")
                .expiryDate(0)
                .issueDate(0)
                .issuedTo("elasticsearch")
                .issuer("issuer")
                .type("cloud_internal")
                .maxNodes(1)
                .build();

        assertThat(license.operationMode(), equalTo(License.OperationMode.PLATINUM));
        OperationModeFileWatcherTests.writeMode("gold", licenseModeFile);
        license.setOperationModeFileWatcher(operationModeFileWatcher);
        verify(resourceWatcherService, times(1)).add(any(FileWatcher.class), eq(ResourceWatcherService.Frequency.HIGH));
        assertThat(license.operationMode(), equalTo(License.OperationMode.GOLD));
    }
}
