/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.vectors;

import org.elasticsearch.index.mapper.SourceLoader;

import java.io.IOException;
import java.util.List;

public interface DenseVectorSyntheticFieldLoader extends SourceLoader.SyntheticFieldLoader {
    /**
     * Get the vector value as a list.
     */
    List<Object> vectorAsList() throws IOException;
}
