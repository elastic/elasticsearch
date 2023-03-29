/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

@LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/94849")
public class KnnByteVectorQueryBuilderTests extends AbstractKnnVectorQueryBuilderTestCase {
    @Override
    DenseVectorFieldMapper.ElementType elementType() {
        return DenseVectorFieldMapper.ElementType.BYTE;
    }
}
