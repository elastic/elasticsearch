/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.mock;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.plugins.Plugin;

import java.util.List;

public class TestInferenceServicePlugin extends Plugin {

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                AbstractTestInferenceService.TestTaskSettings.NAME,
                AbstractTestInferenceService.TestTaskSettings::new
            ),
            new NamedWriteableRegistry.Entry(
                SecretSettings.class,
                AbstractTestInferenceService.TestSecretSettings.NAME,
                AbstractTestInferenceService.TestSecretSettings::new
            ),
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                TestDenseInferenceServiceExtension.TestServiceSettings.NAME,
                TestDenseInferenceServiceExtension.TestServiceSettings::new
            ),
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                TestSparseInferenceServiceExtension.TestServiceSettings.NAME,
                TestSparseInferenceServiceExtension.TestServiceSettings::new
            ),
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                TestRerankingServiceExtension.TestServiceSettings.NAME,
                TestRerankingServiceExtension.TestServiceSettings::new
            ),
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                TestRerankingServiceExtension.TestTaskSettings.NAME,
                TestRerankingServiceExtension.TestTaskSettings::new
            ),
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                TestStreamingCompletionServiceExtension.TestServiceSettings.NAME,
                TestStreamingCompletionServiceExtension.TestServiceSettings::new
            ),
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                TestCompletionServiceExtension.TestServiceSettings.NAME,
                TestCompletionServiceExtension.TestServiceSettings::new
            )
        );
    }
}
