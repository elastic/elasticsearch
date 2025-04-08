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
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormatTests;

public class ES819TSDBDocValuesFormatTests extends ES87TSDBDocValuesFormatTests {

    private final Codec codec = TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat());

    @Override
    protected Codec getCodec() {
        return codec;
    }

}
