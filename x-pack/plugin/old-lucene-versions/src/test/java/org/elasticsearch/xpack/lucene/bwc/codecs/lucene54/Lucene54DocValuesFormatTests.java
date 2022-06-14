/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs.lucene54;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.test.GraalVMThreadsFilter;

@ThreadLeakFilters(filters = { GraalVMThreadsFilter.class })
public class Lucene54DocValuesFormatTests extends BaseDocValuesFormatTestCase {

    private final Codec codec = TestUtil.alwaysDocValuesFormat(new Lucene54DocValuesFormat());

    @Override
    protected Codec getCodec() {
        return codec;
    }
}
