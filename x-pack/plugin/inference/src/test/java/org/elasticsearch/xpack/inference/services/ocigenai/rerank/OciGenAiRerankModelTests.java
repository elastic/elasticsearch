/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.services.ocigenai.rerank;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiService;
import org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiServiceFields.TOP_N;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.COMPARTMENT_ID;
import static org.elasticsearch.xpack.inference.services.ocigenai.OciGenAiTestUtils.REGION_VALUE;
import static org.hamcrest.Matchers.is;

public class OciGenAiRerankModelTests extends ESTestCase {

    public void testUri_IsDerivedFromTheRegion() {
        var model = createModel(null, "cohere.rerank-v3.5", null, null);

        assertThat(
            model.uri(),
            is(URI.create("https://inference.generativeai.us-chicago-1.oci.oraclecloud.com/20231130/actions/rerankText"))
        );
    }

    public void testOf_OverridesTaskSettings() {
        var model = createModel(null, "cohere.rerank-v3.5", 2, true);

        var overridden = OciGenAiRerankModel.of(model, new HashMap<>(Map.of(TOP_N, 5)));

        assertThat(overridden.getTaskSettings().getTopN(), is(5));
        assertThat(overridden.getTaskSettings().getReturnDocuments(), is(true));
        assertThat(overridden.uri(), is(model.uri()));
    }

    public static OciGenAiRerankModel createModel(
        @Nullable String url,
        String modelId,
        @Nullable Integer topN,
        @Nullable Boolean returnDocuments
    ) {
        return new OciGenAiRerankModel(
            "id",
            TaskType.RERANK,
            OciGenAiService.NAME,
            url,
            new OciGenAiRerankServiceSettings(
                OciGenAiTestUtils.commonSettings(REGION_VALUE, COMPARTMENT_ID, modelId, null, null, new RateLimitSettings(1000))
            ),
            new OciGenAiRerankTaskSettings(topN, returnDocuments),
            OciGenAiTestUtils.createSecretSettings(),
            OciGenAiTestUtils.fixedAuthHeader()
        );
    }
}
