/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.gpu;

import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gpu.GPUSupport;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;

import static org.hamcrest.Matchers.containsString;

public class GPUPluginBootstrapCheckTests extends AbstractBootstrapCheckTestCase {

    public void testNodeSettingTrueWithoutGPUFails() {
        Settings settings = Settings.builder()
            .put(GPUPlugin.VECTORS_INDEXING_USE_GPU_NODE_SETTING.getKey(), GPUPlugin.GpuMode.TRUE)
            .build();

        BootstrapContext context = createTestContext(settings, Metadata.EMPTY_METADATA);
        var check = new GPUPlugin.GpuModeBootstrapCheck(new TestBootstrapGPUSupport(false));

        var result = check.check(context);
        assertTrue("Bootstrap check should fail when GPU setting is TRUE but GPU is not supported", result.isFailure());
        assertThat(
            result.getMessage(),
            containsString("vectors.indexing.use_gpu is set to [true], but GPU resources are not accessible on this node")
        );
    }

    public void testNodeSettingTrueWithGPUSucceeds() {
        Settings settings = Settings.builder()
            .put(GPUPlugin.VECTORS_INDEXING_USE_GPU_NODE_SETTING.getKey(), GPUPlugin.GpuMode.TRUE)
            .build();

        BootstrapContext context = createTestContext(settings, Metadata.EMPTY_METADATA);
        var check = new GPUPlugin.GpuModeBootstrapCheck(new TestBootstrapGPUSupport(true));

        var result = check.check(context);
        assertTrue("Bootstrap check should succeed when GPU setting is TRUE and GPU is supported", result.isSuccess());
    }

    public void testNodeSettingAutoWithoutGPUSucceeds() {
        Settings settings = Settings.builder()
            .put(GPUPlugin.VECTORS_INDEXING_USE_GPU_NODE_SETTING.getKey(), GPUPlugin.GpuMode.AUTO)
            .build();

        BootstrapContext context = createTestContext(settings, Metadata.EMPTY_METADATA);
        var check = new GPUPlugin.GpuModeBootstrapCheck(new TestBootstrapGPUSupport(false));

        var result = check.check(context);
        assertTrue("Bootstrap check should succeed when GPU setting is AUTO", result.isSuccess());
    }

    public void testNodeSettingFalseWithoutGPUSucceeds() {
        Settings settings = Settings.builder()
            .put(GPUPlugin.VECTORS_INDEXING_USE_GPU_NODE_SETTING.getKey(), GPUPlugin.GpuMode.FALSE)
            .build();

        BootstrapContext context = createTestContext(settings, Metadata.EMPTY_METADATA);
        var check = new GPUPlugin.GpuModeBootstrapCheck(new TestBootstrapGPUSupport(false));

        var result = check.check(context);
        assertTrue("Bootstrap check should succeed when GPU setting is FALSE", result.isSuccess());
    }

    public void testNodeSettingDefaultAutoSucceeds() {
        // Don't set the setting - use default (AUTO)
        Settings settings = Settings.EMPTY;

        BootstrapContext context = createTestContext(settings, Metadata.EMPTY_METADATA);
        var check = new GPUPlugin.GpuModeBootstrapCheck(new TestBootstrapGPUSupport(randomBoolean()));

        var result = check.check(context);
        assertTrue("Bootstrap check should succeed with default setting (AUTO)", result.isSuccess());
    }

    public void testBootstrapCheckReferenceDocs() {
        var check = new GPUPlugin.GpuModeBootstrapCheck(new TestBootstrapGPUSupport(randomBoolean()));
        assertNotNull("referenceDocs should return a non-null value", check.referenceDocs());
    }

    private record TestBootstrapGPUSupport(boolean isSupported) implements GPUSupport {

        @Override
        public long getTotalGpuMemory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getGpuName() {
            throw new UnsupportedOperationException();
        }
    }
}
