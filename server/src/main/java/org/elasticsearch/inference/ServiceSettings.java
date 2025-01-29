/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.xcontent.ToXContentObject;

public interface ServiceSettings extends ToXContentObject, VersionedNamedWriteable, FilteredXContent {

    /**
     * Similarity used in the service. Will be null if not applicable.
     *
     * @return similarity
     */
    default SimilarityMeasure similarity() {
        return null;
    }

    /**
     * Number of dimensions the service works with. Will be null if not applicable.
     *
     * @return number of dimensions
     */
    default Integer dimensions() {
        return null;
    }

    /**
     * Boolean signifying whether the dimensions were set by the user
     *
     * @return boolean signifying whether the dimensions were set by the user
     */
    default Boolean dimensionsSetByUser() {
        return null;
    }

    /**
     * The data type for the embeddings this service works with. Defaults to null,
     * Text Embedding models should return a non-null value
     *
     * @return the element type
     */
    default DenseVectorFieldMapper.ElementType elementType() {
        return null;
    }

    /**
     * The model to use in the inference endpoint (e.g. text-embedding-ada-002). Sometimes the model is not defined in the service
     * settings. This can happen for external providers (e.g. hugging face, azure ai studio) where the provider requires that the model
     * be chosen when initializing a deployment within their service. In this situation, return null.
     * @return the model used to perform inference or null if the model is not defined
     */
    @Nullable
    String modelId();
}
