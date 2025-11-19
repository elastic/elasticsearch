/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.index.codec.Elasticsearch92Lucene103Codec;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormatTests;

import static org.elasticsearch.test.ESTestCase.randomIntBetween;

public class ES819TSDBLargeBlockDocValuesFormatTests extends ES87TSDBDocValuesFormatTests {

    protected final Codec codec = new Elasticsearch92Lucene103Codec() {

        final ES819TSDBLargeBlockDocValuesFormat docValuesFormat = new ES819TSDBLargeBlockDocValuesFormat(
            randomIntBetween(2, 4096),
            randomIntBetween(1, 512),
            random().nextBoolean()
        );

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return docValuesFormat;
        }
    };
}
