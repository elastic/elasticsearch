/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsFormat;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.elasticsearch.core.SuppressForbidden;

import static org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper.MAX_DIMS_COUNT;

public abstract class AbstractFlatVectorsFormat extends FlatVectorsFormat {

    public static final boolean USE_DIRECT_IO = getUseDirectIO();

    @SuppressForbidden(
        reason = "TODO Deprecate any lenient usage of Boolean#parseBoolean https://github.com/elastic/elasticsearch/issues/128993"
    )
    private static boolean getUseDirectIO() {
        return Boolean.parseBoolean(System.getProperty("vector.rescoring.directio", "false"));
    }

    protected AbstractFlatVectorsFormat(String name) {
        super(name);
    }

    protected abstract FlatVectorsScorer flatVectorsScorer();

    @Override
    public int getMaxDimensions(String fieldName) {
        return MAX_DIMS_COUNT;
    }

    @Override
    public String toString() {
        return getName() + "(name=" + getName() + ", flatVectorScorer=" + flatVectorsScorer() + ")";
    }
}
