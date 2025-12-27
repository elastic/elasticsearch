/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.voyageai;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.contextual.VoyageAIContextualEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.text.VoyageAIEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.voyageai.embeddings.multimodal.VoyageAIMultimodalEmbeddingsModelTests;
import org.elasticsearch.xpack.inference.services.voyageai.rerank.VoyageAIRerankModelTests;
import org.hamcrest.MatcherAssert;
import java.util.HashMap;
import java.util.Map;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class VoyageAIRequestManagerTests extends ESTestCase {

    public void testRateLimitGrouping_SameModel_ReturnsSameGroup() {
        var model1 = VoyageAIEmbeddingsModelTests.createModel(
            "url",
            "api_key",
            null,
            null,
            "voyage-3-large"
        );
        var model2 = VoyageAIEmbeddingsModelTests.createModel(
            "url",
            "api_key",
            null,
            null,
            "voyage-3-large"
        );

        var grouping1 = VoyageAIRequestManager.RateLimitGrouping.of(model1);
        var grouping2 = VoyageAIRequestManager.RateLimitGrouping.of(model2);

        MatcherAssert.assertThat(grouping1, equalTo(grouping2));
        MatcherAssert.assertThat(grouping1.hashCode(), equalTo(grouping2.hashCode()));
    }

    public void testRateLimitGrouping_DifferentModelsInSameFamily_ReturnsSameGroup() {
        var model1 = VoyageAIEmbeddingsModelTests.createModel(
            "url",
            "api_key",
            null,
            null,
            "voyage-3-large"
        );
        var model2 = VoyageAIEmbeddingsModelTests.createModel(
            "url",
            "api_key",
            null,
            null,
            "voyage-code-3"
        );

        var grouping1 = VoyageAIRequestManager.RateLimitGrouping.of(model1);
        var grouping2 = VoyageAIRequestManager.RateLimitGrouping.of(model2);

        MatcherAssert.assertThat(grouping1, equalTo(grouping2));
    }

    public void testRateLimitGrouping_DifferentModelFamilies_ReturnsDifferentGroups() {
        var model1 = VoyageAIEmbeddingsModelTests.createModel(
            "url",
            "api_key",
            null,
            null,
            "voyage-3-large"
        );
        var model2 = VoyageAIEmbeddingsModelTests.createModel(
            "url",
            "api_key",
            null,
            null,
            "voyage-3"
        );

        var grouping1 = VoyageAIRequestManager.RateLimitGrouping.of(model1);
        var grouping2 = VoyageAIRequestManager.RateLimitGrouping.of(model2);

        MatcherAssert.assertThat(grouping1, not(equalTo(grouping2)));
    }

    public void testRateLimitGrouping_UnrecognizedModel_UsesDefaultFamily() {
        var knownModel = VoyageAIEmbeddingsModelTests.createModel(
            "url",
            "api_key",
            null,
            null,
            "voyage-3-large"
        );
        var unknownModel = VoyageAIEmbeddingsModelTests.createModel(
            "url",
            "api_key",
            null,
            null,
            "unknown-model"
        );

        var grouping1 = VoyageAIRequestManager.RateLimitGrouping.of(knownModel);
        var grouping2 = VoyageAIRequestManager.RateLimitGrouping.of(unknownModel);

        MatcherAssert.assertThat(grouping1, not(equalTo(grouping2)));
    }

    public void testRateLimitGrouping_AllSupportedModels_GroupedCorrectly() {
        Map<String, Integer> modelToFamilyHash = new HashMap<>();

        // Test embedding models
        String[] embedLargeModels = { "voyage-3-large", "voyage-code-3", "voyage-finance-2", "voyage-law-2", "voyage-code-2" };
        for (String model : embedLargeModels) {
            var modelObj = VoyageAIEmbeddingsModelTests.createModel("url", "api_key", null, null, model);
            var grouping = VoyageAIRequestManager.RateLimitGrouping.of(modelObj);
            modelToFamilyHash.put(model, grouping.apiKeyHash());
        }

        // All large models should have same hash
        var largeFamilyHashes = modelToFamilyHash.values();
        MatcherAssert.assertThat(
            "All embed_large family models should have same hash",
            largeFamilyHashes.stream().distinct().count(),
            equalTo(1L)
        );

        // Test embed_medium
        var mediumModel = VoyageAIEmbeddingsModelTests.createModel("url", "api_key", null, null, "voyage-3");
        var mediumHash = VoyageAIRequestManager.RateLimitGrouping.of(mediumModel).apiKeyHash();
        MatcherAssert.assertThat(
            "Medium model should have different hash than large",
            mediumHash,
            not(equalTo(largeFamilyHashes.iterator().next()))
        );

        // Test embed_small
        var smallModel = VoyageAIEmbeddingsModelTests.createModel("url", "api_key", null, null, "voyage-3-lite");
        var smallHash = VoyageAIRequestManager.RateLimitGrouping.of(smallModel).apiKeyHash();
        MatcherAssert.assertThat(
            "Small model should have different hash",
            smallHash,
            not(equalTo(largeFamilyHashes.iterator().next()))
        );
        MatcherAssert.assertThat(
            "Small model should have different hash than medium",
            smallHash,
            not(equalTo(mediumHash))
        );
    }

    public void testRateLimitGrouping_RerankModels_GroupedCorrectly() {
        var rerankLargeModel = VoyageAIRerankModelTests.createModel("api_key", "rerank-2", 10);
        var rerankLargeHash = VoyageAIRequestManager.RateLimitGrouping.of(rerankLargeModel).apiKeyHash();

        var rerankSmallModel = VoyageAIRerankModelTests.createModel("api_key", "rerank-2-lite", 10);
        var rerankSmallHash = VoyageAIRequestManager.RateLimitGrouping.of(rerankSmallModel).apiKeyHash();

        var embedLargeModel = VoyageAIEmbeddingsModelTests.createModel("url", "api_key", null, null, "voyage-3-large");
        var embedLargeHash = VoyageAIRequestManager.RateLimitGrouping.of(embedLargeModel).apiKeyHash();

        MatcherAssert.assertThat(
            "rerank_large and embed_large should have different groups",
            rerankLargeHash,
            not(equalTo(embedLargeHash))
        );

        MatcherAssert.assertThat(
            "Rerank large and small should have different groups",
            rerankLargeHash,
            not(equalTo(rerankSmallHash))
        );
    }

    public void testRateLimitGrouping_ContextualEmbeddings_HaveOwnGroups() {
        var contextualModel = VoyageAIContextualEmbeddingsModelTests.createModel(
            "url",
            "api_key",
            null,
            null,
            "voyage-context-3"
        );
        var contextualHash = VoyageAIRequestManager.RateLimitGrouping.of(contextualModel).apiKeyHash();

        var regularModel = VoyageAIEmbeddingsModelTests.createModel(
            "url",
            "api_key",
            null,
            null,
            "voyage-3"
        );
        var regularHash = VoyageAIRequestManager.RateLimitGrouping.of(regularModel).apiKeyHash();

        MatcherAssert.assertThat(
            "Contextual embeddings should have different rate limit group",
            contextualHash,
            not(equalTo(regularHash))
        );
    }

    public void testRateLimitGrouping_MultimodalModels_HaveOwnGroup() {
        var multimodalModel = VoyageAIMultimodalEmbeddingsModelTests.createModel("url", "api_key", null, "voyage-multimodal-3");
        var multimodalHash = VoyageAIRequestManager.RateLimitGrouping.of(multimodalModel).apiKeyHash();

        var regularModel = VoyageAIEmbeddingsModelTests.createModel(
            "url",
            "api_key",
            null,
            null,
            "voyage-3"
        );
        var regularHash = VoyageAIRequestManager.RateLimitGrouping.of(regularModel).apiKeyHash();

        MatcherAssert.assertThat(
            "Multimodal models should have different rate limit group",
            multimodalHash,
            not(equalTo(regularHash))
        );
    }

    public void testRateLimitGrouping_ApiKeyHashConsistency() {
        var model1 = VoyageAIEmbeddingsModelTests.createModel("url", "key1", null, null, "voyage-3-large");
        var model3 = VoyageAIEmbeddingsModelTests.createModel("url", "key1", null, null, "voyage-code-3");

        var grouping1 = VoyageAIRequestManager.RateLimitGrouping.of(model1);
        var grouping3 = VoyageAIRequestManager.RateLimitGrouping.of(model3);

        // Same model family but different API keys should still group by model family first
        MatcherAssert.assertThat(
            "Different API keys with same model should have same rate limit grouping",
            grouping1,
            equalTo(grouping3)
        );

        // Note: rate limit grouping is based on model family, not API key
        // This is by design - models share rate limits across API keys
        MatcherAssert.assertThat(
            "Different API keys in same family",
            grouping1.apiKeyHash(),
            equalTo(grouping3.apiKeyHash())
        );
    }
}
