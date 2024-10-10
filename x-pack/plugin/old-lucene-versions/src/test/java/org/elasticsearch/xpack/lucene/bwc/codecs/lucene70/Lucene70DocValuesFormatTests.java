/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.lucene.bwc.codecs.lucene70;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.index.LegacyBaseDocValuesFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.test.GraalVMThreadsFilter;

@ThreadLeakFilters(filters = { GraalVMThreadsFilter.class })
public class Lucene70DocValuesFormatTests extends LegacyBaseDocValuesFormatTestCase {

    private final Codec codec = TestUtil.alwaysDocValuesFormat(new Lucene70DocValuesFormat());

    @Override
    protected Codec getCodec() {
        return codec;
    }
}
