/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.pytorch.process;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.process.NativeController;
import org.elasticsearch.xpack.ml.process.ProcessPipes;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class PyTorchBuilderTests extends ESTestCase {

    private static final String PROCESS_PIPES_ARG = "--process_pipes_test_arg";

    private NativeController nativeController;
    private ProcessPipes processPipes;
    private ArgumentCaptor<List<String>> commandCaptor;

    @SuppressWarnings("unchecked")
    @Before
    public void setUpMocks() {
        nativeController = mock(NativeController.class);
        processPipes = mock(ProcessPipes.class);
        commandCaptor = ArgumentCaptor.forClass((Class) List.class);
        doAnswer(invocationOnMock -> {
            List<String> command = (List<String>) invocationOnMock.getArguments()[0];
            command.add(PROCESS_PIPES_ARG);
            return null;
        }).when(processPipes).addArgs(any());
    }

    public void testBuild() throws IOException, InterruptedException {
        new PyTorchBuilder(nativeController, processPipes, 2, 4, 12).build();

        verify(nativeController).startProcess(commandCaptor.capture());

        assertThat(
            commandCaptor.getValue(),
            contains(
                "./pytorch_inference",
                "--validElasticLicenseKeyConfirmed=true",
                "--numThreadsPerAllocation=2",
                "--numAllocations=4",
                "--cacheMemorylimitBytes=12",
                PROCESS_PIPES_ARG
            )
        );
    }

    public void testBuildWithNoCache() throws IOException, InterruptedException {
        new PyTorchBuilder(nativeController, processPipes, 2, 4, 0).build();

        verify(nativeController).startProcess(commandCaptor.capture());

        assertThat(
            commandCaptor.getValue(),
            contains(
                "./pytorch_inference",
                "--validElasticLicenseKeyConfirmed=true",
                "--numThreadsPerAllocation=2",
                "--numAllocations=4",
                PROCESS_PIPES_ARG
            )
        );
    }
}
