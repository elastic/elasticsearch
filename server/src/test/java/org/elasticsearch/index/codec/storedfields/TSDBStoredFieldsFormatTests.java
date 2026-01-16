/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.storedfields;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.tests.index.BaseStoredFieldsFormatTestCase;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.bloomfilter.ES93BloomFilterStoredFieldsFormat;
import org.elasticsearch.index.mapper.IdFieldMapper;

public class TSDBStoredFieldsFormatTests extends BaseStoredFieldsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    @Override
    protected Codec getCodec() {
        var bloomFilterSizeInKb = atLeast(1);
        var tsdbStoredFieldsFormat = new TSDBStoredFieldsFormat(
            new Lucene90StoredFieldsFormat(),
            new ES93BloomFilterStoredFieldsFormat(
                BigArrays.NON_RECYCLING_INSTANCE,
                ByteSizeValue.ofKb(bloomFilterSizeInKb),
                IdFieldMapper.NAME
            )
        );
        return new AssertingCodec() {
            @Override
            public StoredFieldsFormat storedFieldsFormat() {
                return tsdbStoredFieldsFormat;
            }
        };
    }
}
