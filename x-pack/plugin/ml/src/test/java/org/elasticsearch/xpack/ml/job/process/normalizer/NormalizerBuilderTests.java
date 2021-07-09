/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.job.process.normalizer;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectBuilder;

import java.io.IOException;
import java.util.List;

public class NormalizerBuilderTests extends ESTestCase {

    public void testBuildNormalizerCommand() throws IOException {
        Environment env = TestEnvironment.newEnvironment(
                Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build());
        String jobId = "unit-test-job";

        List<String> command = new NormalizerBuilder(env, jobId, null, 300).build();
        assertEquals(4, command.size());
        assertTrue(command.contains("./normalize"));
        assertTrue(command.contains(NormalizerBuilder.BUCKET_SPAN_ARG + "300"));
        assertTrue(command.contains(AutodetectBuilder.LENGTH_ENCODED_INPUT_ARG));
        assertTrue(command.contains(AutodetectBuilder.LICENSE_KEY_VALIDATED_ARG + true));
    }
}
