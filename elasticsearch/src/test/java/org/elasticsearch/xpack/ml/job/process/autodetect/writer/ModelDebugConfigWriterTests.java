/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.io.OutputStreamWriter;

import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;
import org.elasticsearch.xpack.ml.job.config.ModelDebugConfig;

public class ModelDebugConfigWriterTests extends ESTestCase {
    private OutputStreamWriter writer;

    @Before
    public void setUpMocks() {
        writer = Mockito.mock(OutputStreamWriter.class);
    }

    @After
    public void verifyNoMoreWriterInteractions() {
        verifyNoMoreInteractions(writer);
    }
    public void testWrite_GivenFullConfig() throws IOException {
        ModelDebugConfig modelDebugConfig = new ModelDebugConfig(65.0, "foo,bar");
        ModelDebugConfigWriter writer = new ModelDebugConfigWriter(modelDebugConfig, this.writer);

        writer.write();

        verify(this.writer).write("boundspercentile = 65.0\nterms = foo,bar\n");
    }
}
