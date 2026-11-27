/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.zstd;

import com.carrotsearch.randomizedtesting.annotations.Listeners;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.index.BaseStoredFieldsFormatTestCase;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.bwc.Elasticsearch93Lucene104Codec;
import org.elasticsearch.test.junit.listener.ReproduceInfoPrinter;
import org.elasticsearch.test.junit.runner.ESRandomizedRunner;
import org.junit.runner.RunWith;

@Listeners(ReproduceInfoPrinter.class)
@LuceneTestCase.SuppressReproduceLine
@RunWith(ESRandomizedRunner.class)
public class Zstd814BestCompressionStoredFieldsFormatTests extends BaseStoredFieldsFormatTestCase {

    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    private final Codec codec = new Elasticsearch93Lucene104Codec(Zstd814StoredFieldsFormat.Mode.BEST_COMPRESSION);

    @Override
    protected Codec getCodec() {
        return codec;
    }
}
