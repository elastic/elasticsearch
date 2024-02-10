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
import org.elasticsearch.xpack.core.inference.results.ChunkedSparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.ChunkedTextEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.LegacyTextEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.SparseEmbeddingResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingByteResults;
import org.elasticsearch.xpack.core.inference.results.TextEmbeddingResults;
import org.elasticsearch.xpack.inference.services.cohere.CohereServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.cohere.embeddings.CohereEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.elser.ElserInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.elser.ElserMlNodeTaskSettings;
import org.elasticsearch.xpack.inference.services.huggingface.HuggingFaceServiceSettings;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserSecretSettings;
import org.elasticsearch.xpack.inference.services.huggingface.elser.HuggingFaceElserServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsServiceSettings;
import org.elasticsearch.xpack.inference.services.openai.embeddings.OpenAiEmbeddingsTaskSettings;
import org.elasticsearch.xpack.inference.services.settings.DefaultSecretSettings;
import org.elasticsearch.xpack.inference.services.textembedding.MultilingualE5SmallInternalServiceSettings;
import org.elasticsearch.xpack.inference.services.textembedding.TextEmbeddingInternalServiceSettings;

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
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(InferenceServiceResults.class, TextEmbeddingByteResults.NAME, TextEmbeddingByteResults::new)
        );

        // Chunked inference results
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceServiceResults.class,
                ChunkedSparseEmbeddingResults.NAME,
                ChunkedSparseEmbeddingResults::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                InferenceServiceResults.class,
                ChunkedTextEmbeddingResults.NAME,
                ChunkedTextEmbeddingResults::new
            )
        );
        // TODO add text embedding byte result

        // Empty default task settings
        namedWriteables.add(new NamedWriteableRegistry.Entry(TaskSettings.class, EmptyTaskSettings.NAME, EmptyTaskSettings::new));

        // Default secret settings
        namedWriteables.add(new NamedWriteableRegistry.Entry(SecretSettings.class, DefaultSecretSettings.NAME, DefaultSecretSettings::new));

        // Internal ELSER config
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, ElserInternalServiceSettings.NAME, ElserInternalServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, ElserMlNodeTaskSettings.NAME, ElserMlNodeTaskSettings::new)
        );

        // Internal TextEmbedding service config
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                TextEmbeddingInternalServiceSettings.NAME,
                TextEmbeddingInternalServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                MultilingualE5SmallInternalServiceSettings.NAME,
                MultilingualE5SmallInternalServiceSettings::new
            )
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
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                OpenAiEmbeddingsServiceSettings.NAME,
                OpenAiEmbeddingsServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, OpenAiEmbeddingsTaskSettings.NAME, OpenAiEmbeddingsTaskSettings::new)
        );

        // Cohere
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(ServiceSettings.class, CohereServiceSettings.NAME, CohereServiceSettings::new)
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(
                ServiceSettings.class,
                CohereEmbeddingsServiceSettings.NAME,
                CohereEmbeddingsServiceSettings::new
            )
        );
        namedWriteables.add(
            new NamedWriteableRegistry.Entry(TaskSettings.class, CohereEmbeddingsTaskSettings.NAME, CohereEmbeddingsTaskSettings::new)
        );

        return namedWriteables;
    }
}
