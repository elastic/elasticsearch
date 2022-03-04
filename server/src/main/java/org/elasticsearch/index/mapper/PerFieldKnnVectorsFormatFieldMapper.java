/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.apache.lucene.codecs.KnnVectorsFormat;

/**
 * Field mapper used for the only purpose to provide a custom knn vectors format.
 * For internal use only.
 */

public interface PerFieldKnnVectorsFormatFieldMapper {

    /**
    * Returns the knn vectors format that is customly set up for this field
     * or {@code null} if the format is not set up.
    * @return the knn vectors format for the field, or {@code null} if the default format should be used
    */
    KnnVectorsFormat getKnnVectorsFormatForField();
}
