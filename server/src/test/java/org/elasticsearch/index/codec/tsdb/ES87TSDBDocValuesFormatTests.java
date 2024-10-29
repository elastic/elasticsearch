/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.test.MockLog;

import java.io.IOException;

public class ES87TSDBDocValuesFormatTests extends ES817TSDBDocValuesFormatTests {

    private final Codec codec = TestUtil.alwaysDocValuesFormat(new TestES87TSDBDocValuesFormat());

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging();
        MockLog.init();
    }

    @Override
    protected Codec getCodec() {
        return codec;
    }

    static class TestES87TSDBDocValuesFormat extends ES87TSDBDocValuesFormat {
        @Override
        public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
            return new ES87TSDBDocValuesConsumer(state, random().nextInt(4, 16), DATA_CODEC, DATA_EXTENSION, META_CODEC, META_EXTENSION);
        }
    }
}
