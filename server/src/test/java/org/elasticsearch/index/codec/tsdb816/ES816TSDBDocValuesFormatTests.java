/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb816;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.index.BaseDocValuesFormatTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class ES816TSDBDocValuesFormatTests extends BaseDocValuesFormatTestCase {

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysDocValuesFormat(new ES816TSDBDocValuesFormat());
    }

}
