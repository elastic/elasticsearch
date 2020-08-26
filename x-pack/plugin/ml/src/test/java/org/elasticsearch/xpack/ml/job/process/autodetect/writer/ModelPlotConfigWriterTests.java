/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.ModelPlotConfig;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.OutputStreamWriter;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class ModelPlotConfigWriterTests extends ESTestCase {
    private OutputStreamWriter writer;

    @Before
    public void setUpMocks() {
        writer = Mockito.mock(OutputStreamWriter.class);
    }

    @After
    public void verifyNoMoreWriterInteractions() {
        verifyNoMoreInteractions(writer);
    }

    public void testWrite_GivenEnabledConfigWithoutTerms() throws IOException {
        ModelPlotConfig modelPlotConfig = new ModelPlotConfig();
        ModelPlotConfigWriter writer = new ModelPlotConfigWriter(modelPlotConfig, this.writer);

        writer.write();

        verify(this.writer).write("boundspercentile = 95.0\nterms = \nannotations_enabled = true\n");
    }

    public void testWrite_GivenEnabledConfigWithTerms() throws IOException {
        ModelPlotConfig modelPlotConfig = new ModelPlotConfig(true, "foo,bar", false);
        ModelPlotConfigWriter writer = new ModelPlotConfigWriter(modelPlotConfig, this.writer);

        writer.write();

        verify(this.writer).write("boundspercentile = 95.0\nterms = foo,bar\nannotations_enabled = false\n");
    }

    public void testWrite_GivenDisabledConfigWithTerms() throws IOException {
        ModelPlotConfig modelPlotConfig = new ModelPlotConfig(false, "foo,bar", false);
        ModelPlotConfigWriter writer = new ModelPlotConfigWriter(modelPlotConfig, this.writer);

        writer.write();

        verify(this.writer).write("boundspercentile = -1.0\nterms = foo,bar\nannotations_enabled = false\n");
    }

    public void testWrite_GivenEnabledConfigWithEnabledAnnotations() throws IOException {
        ModelPlotConfig modelPlotConfig = new ModelPlotConfig(true, null, true);
        ModelPlotConfigWriter writer = new ModelPlotConfigWriter(modelPlotConfig, this.writer);

        writer.write();

        verify(this.writer).write("boundspercentile = 95.0\nterms = \nannotations_enabled = true\n");
    }

    public void testWrite_GivenDisabledConfigWithEnabledAnnotations() throws IOException {
        ModelPlotConfig modelPlotConfig = new ModelPlotConfig(false, null, true);
        ModelPlotConfigWriter writer = new ModelPlotConfigWriter(modelPlotConfig, this.writer);

        writer.write();

        verify(this.writer).write("boundspercentile = -1.0\nterms = \nannotations_enabled = true\n");
    }
}
