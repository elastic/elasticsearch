/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.dataframe.process;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class AnalyticsBuilderTests extends ESTestCase {

    private NativeController nativeController;
    private ProcessPipes processPipes;
    private AnalyticsProcessConfig config;
    private List<Path> filesToDelete;
    private ArgumentCaptor<List<String>> commandCaptor;
    private AnalyticsBuilder analyticsBuilder;

    @SuppressWarnings("unchecked")
    @Before
    public void setUpMocks() {
        nativeController = mock(NativeController.class);
        processPipes = mock(ProcessPipes.class);
        config = mock(AnalyticsProcessConfig.class);
        filesToDelete = new ArrayList<>();
        commandCaptor = ArgumentCaptor.forClass((Class) List.class);

        analyticsBuilder = new AnalyticsBuilder(LuceneTestCase::createTempDir, nativeController, processPipes, config, filesToDelete);
    }

    public void testBuild_Analytics() throws Exception {
        analyticsBuilder.build();
        assertThat(filesToDelete, hasSize(1));

        verify(nativeController).startProcess(commandCaptor.capture());
        verifyNoMoreInteractions(nativeController);

        List<String> command = commandCaptor.getValue();
        assertThat(command, not(hasItem("--memoryUsageEstimationOnly")));
        assertThat(command, hasItem("--validElasticLicenseKeyConfirmed=true"));
    }

    public void testBuild_MemoryUsageEstimation() throws Exception {
        analyticsBuilder.performMemoryUsageEstimationOnly().build();
        assertThat(filesToDelete, hasSize(1));

        verify(nativeController).startProcess(commandCaptor.capture());
        verifyNoMoreInteractions(nativeController);

        List<String> command = commandCaptor.getValue();
        assertThat(command, hasItem("--memoryUsageEstimationOnly"));
        assertThat(command, hasItem("--validElasticLicenseKeyConfirmed=true"));
    }
}
