/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.inference.EmptyTaskSettings;
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.SecretSettings;
import org.elasticsearch.inference.ServiceSettings;
import org.elasticsearch.inference.TaskSettings;
import org.elasticsearch.xpack.core.inference.results.LegacyTextEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeServiceSettings;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeTaskSettings;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceServiceSettings;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserSecretSettings;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.OpenAiServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;

import java.util.ArrayList;
import java.util.List;

public class InferenceNamedWriteablesProvider {

    private InferenceNamedWriteablesProvider() {}

    @SuppressWarnings("deprecation")
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> namedWriteables = new ArrayList<>();

        // Legacy inference results
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceResults.class, LegacyTextEmbeddingResults.NAME, LegacyTextEmbeddingResults::new)
        );

        // Inference results
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceServiceResults.class, SparseEmbeddingResults.NAME, SparseEmbeddingResults::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceServiceResults.class, TextEmbeddingResults.NAME, TextEmbeddingResults::new)
        );

        // Empty default task settings
        namedWriteables.add(new NamedWriteableRegistry.Entry(TaskSettings.class, EmptyTaskSettings.NAME, EmptyTaskSettings::new));

        // Default secret settings
        namedWriteables.add(new NamedWriteableRegistry.Entry(SecretSettings.class, DefaultSecretSettings.NAME, DefaultSecretSettings::new));

        // ELSER config
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, ElserMlNodeServiceSettings.NAME, ElserMlNodeServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, ElserMlNodeTaskSettings.NAME, ElserMlNodeTaskSettings::new)
        );

        // Hugging Face config
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                HuggingFaceElserServiceSettings.NAME,
                HuggingFaceElserServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, HuggingFaceServiceSettings.NAME, HuggingFaceServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(SecretSettings.class, HuggingFaceElserSecretSettings.NAME, HuggingFaceElserSecretSettings::new)
        );

        // OpenAI
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, OpenAiServiceSettings.NAME, OpenAiServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, OpenAiEmbeddingsTaskSettings.NAME, OpenAiEmbeddingsTaskSettings::new)
        );

        return namedWriteables;
    }
}
