/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene100.Lucene100Codec;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.elasticsearch.common.logging.LogConfigurator;

public class ES813FlatVectorFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Override
    protected Codec getCodec() {
        return new Lucene100Codec() {
            @Override
            public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                return new ES813FlatVectorFormat();
            }
        };
    }

    public void testSearchWithVisitedLimit() {
        assumeTrue("requires graph based vector codec", false);
    }

}
