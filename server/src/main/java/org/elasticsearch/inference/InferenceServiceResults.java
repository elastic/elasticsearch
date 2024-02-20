/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xcontent.ToXContentFragment;

import java.util.List;
import java.util.Map;

public interface InferenceServiceResults extends NamedWriteable, ToXContentFragment {

    /**
     * Transform the result to match the format required for the TransportCoordinatedInferenceAction.
     * For the inference plugin TextEmbeddingResults, the {@link #transformToLegacyFormat()} transforms the
     * results into an intermediate format only used by the plugin's return value. It doesn't align with what the
     * TransportCoordinatedInferenceAction expects. TransportCoordinatedInferenceAction expects an ml plugin
     * TextEmbeddingResults.
     *
     * For other results like SparseEmbeddingResults, this method can be a pass through to the transformToLegacyFormat.
     */
    List<? extends InferenceResults> transformToCoordinationFormat();

    /**
     * Transform the result to match the format required for versions prior to
     * {@link org.elasticsearch.TransportVersions#V_8_12_0}
     */
    List<? extends InferenceResults> transformToLegacyFormat();

    /**
     * Retrieves a map representation of the results. It should be equivalent to parsing the
     * XContent representation of the results.
     *
     * @return the results as a map
     */
    Map<String, Object> asMap();
}
