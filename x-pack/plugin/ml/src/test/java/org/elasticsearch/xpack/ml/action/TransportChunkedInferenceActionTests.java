/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.action.ChunkedInferenceAction;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TokenizationConfigUpdate;

import static org.hamcrest.Matchers.instanceOf;

public class TransportChunkedInferenceActionTests extends ESTestCase {

    public void testTranslateRequest() {
        var modelId = "chunking_model";
        var inputs = randomList(1, 6, () -> randomAlphaOfLength(4));
        Integer windowSize = randomBoolean() ? null : randomIntBetween(8, 16);
        Integer span = randomBoolean() ? null : randomIntBetween(4, 7);
        TimeValue timeout = TimeValue.timeValueMillis(randomMillisUpToYear9999());
        var chunkRequest = new ChunkedInferenceAction.Request(modelId, inputs, windowSize, span, timeout);

        var inferRequest = TransportChunkedInferenceAction.translateRequest(chunkRequest);
        assertEquals(modelId, inferRequest.getId());
        assertEquals(inputs, inferRequest.getTextInput());
        assertThat(inferRequest.getUpdate(), instanceOf(TokenizationConfigUpdate.class));
        var tokenizationUpdate = (TokenizationConfigUpdate) inferRequest.getUpdate();
        assertEquals(windowSize, tokenizationUpdate.getSpanSettings().maxSequenceLength());
        assertEquals((int) span, tokenizationUpdate.getSpanSettings().span());
        assertEquals(timeout, inferRequest.getInferenceTimeout());
        assertEquals(TrainedModelPrefixStrings.PrefixType.INGEST, inferRequest.getPrefixType());
        assertFalse(inferRequest.isHighPriority());
        assertTrue(inferRequest.isChunkResults());
    }
}
