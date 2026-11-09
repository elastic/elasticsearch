/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.dv;

import org.apache.lucene.codecs.DocValuesFormat;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.instanceOf;

/** A segment stores the format name only, so Lucene must be able to find the format by that name through SPI. */
public class ParquetDocValuesFormatSpiTests extends ESTestCase {

    public void testFormatIsFoundByName() {
        assertThat(DocValuesFormat.forName(ParquetDocValuesFormat.NAME), instanceOf(ParquetDocValuesFormat.class));
    }
}
