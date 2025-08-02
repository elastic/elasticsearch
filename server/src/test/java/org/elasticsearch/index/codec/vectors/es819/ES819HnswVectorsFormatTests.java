/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es819;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.nativeaccess.NativeAccess;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class ES819HnswVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    static final boolean optimizedScorer = NativeAccess.instance().getVectorSimilarityFunctions().isPresent();

    static final Codec codec = TestUtil.alwaysKnnVectorsFormat(new ES819HnswVectorsFormat());

    @Override
    protected Codec getCodec() {
        return codec;
    }

    public void testToString() {
        FilterCodec customCodec = new FilterCodec("foo", Codec.getDefault()) {
            @Override
            public KnnVectorsFormat knnVectorsFormat() {
                return new ES819HnswVectorsFormat(10, 20);
            }
        };
        var expectedScorer = optimizedScorer
            ? "ESFlatVectorsScorer(delegate=%luceneScorer%, factory=VectorScorerFactoryImpl)"
            : "%luceneScorer%";
        String expectedPattern = "ES819HnswVectorsFormat(name=ES819HnswVectorsFormat, maxConn=10, beamWidth=20,"
            + " flatVectorFormat=Lucene99FlatVectorsFormat(vectorsScorer=%scorer%))".replace("%scorer%", expectedScorer);

        var defaultScorer = expectedPattern.replace("%luceneScorer%", "DefaultFlatVectorScorer()");
        var memSegScorer = expectedPattern.replace("%luceneScorer%", "Lucene99MemorySegmentFlatVectorsScorer()");
        assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
    }
}
