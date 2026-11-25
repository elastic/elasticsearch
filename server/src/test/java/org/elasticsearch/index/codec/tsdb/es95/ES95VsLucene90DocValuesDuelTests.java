/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesDuelTests;

/** Duels ES95 against Lucene90 to verify correctness against the reference implementation. */
public class ES95VsLucene90DocValuesDuelTests extends AbstractTSDBDocValuesDuelTests {

    @Override
    protected DocValuesFormat baselineFormat() {
        return new Lucene90DocValuesFormat();
    }

    @Override
    protected DocValuesFormat contenderFormat() {
        return new ES95TSDBDocValuesFormat();
    }
}
