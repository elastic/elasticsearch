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
                ServiceSettings.class,
                TestInferenceServiceExtension.TestServiceSettings.NAME,
                TestInferenceServiceExtension.TestServiceSettings::new
            ),
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                TestInferenceServiceExtension.TestTaskSettings.NAME,
                TestInferenceServiceExtension.TestTaskSettings::new
            ),
            new NamedWriteableRegistry.Entry(
                SecretSettings.class,
                TestInferenceServiceExtension.TestSecretSettings.NAME,
                TestInferenceServiceExtension.TestSecretSettings::new
            ),
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                TestInferenceTextEmbeddingServiceExtension.TestServiceSettings.NAME,
                TestInferenceTextEmbeddingServiceExtension.TestServiceSettings::new
            ),
            new NamedWriteableRegistry.Entry(
                TaskSettings.class,
                TestInferenceTextEmbeddingServiceExtension.TestTaskSettings.NAME,
                TestInferenceTextEmbeddingServiceExtension.TestTaskSettings::new
            ),
            new NamedWriteableRegistry.Entry(
                SecretSettings.class,
                TestInferenceTextEmbeddingServiceExtension.TestSecretSettings.NAME,
                TestInferenceTextEmbeddingServiceExtension.TestSecretSettings::new
            )
        );
    }
}
