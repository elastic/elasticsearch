/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import java.io.IOException;
import java.util.List;

public interface DenseVectorDocValuesFieldLoader {
    /**
     * Get the vector value as a list.
     *
     * @param convertToFloat whether to convert dimensions to {@code Float}
     */
    List<?> vectorAsList(boolean convertToFloat) throws IOException;
}
