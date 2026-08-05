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
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesDuelTests;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;

/** Duels ES95 against ES819 to verify correctness against the codec it replaces. */
public class ES95VsES819DocValuesDuelTests extends AbstractTSDBDocValuesDuelTests {

    @Override
    protected DocValuesFormat baselineFormat() {
        return new ES819TSDBDocValuesFormat();
    }

    @Override
    protected DocValuesFormat contenderFormat() {
        return new ES95TSDBDocValuesFormat();
    }
}
