/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.test.ESTestCase;

public class ParquetDocValuesFormatSpiTests extends ESTestCase {

    public void testResolvedByNameThroughSpi() {
        DocValuesFormat format = DocValuesFormat.forName(ParquetDocValuesFormat.NAME);
        assertEquals(ParquetDocValuesFormat.class, format.getClass());
    }
}
