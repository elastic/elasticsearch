/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.zstd;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.index.BaseStoredFieldsFormatTestCase;
import org.elasticsearch.index.codec.Elasticsearch814Codec;

public class Zstd814BestCompressionStoredFieldsFormatTests extends BaseStoredFieldsFormatTestCase {

    private final Codec codec = new Elasticsearch814Codec(Zstd814StoredFieldsFormat.Mode.BEST_COMPRESSION);

    @Override
    protected Codec getCodec() {
        return codec;
    }
}
