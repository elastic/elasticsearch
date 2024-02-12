/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;

public class ES813Int8FlatVectorFormatTests extends BaseKnnVectorsFormatTestCase {
    @Override
    protected Codec getCodec() {
        return new Lucene99Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new ES813Int8FlatVectorFormat();
            }
        };
    }

    public void testSearchWithVisitedLimit() {
        assumeTrue("requires graph based vector codec", false);
    }

}
