/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.zstd;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.tests.index.BaseStoredFieldsFormatTestCase;
import org.elasticsearch.index.codec.Elasticsearch92Lucene103Codec;

public class Zstd814BestCompressionStoredFieldsFormatTests extends BaseStoredFieldsFormatTestCase {

    private final Codec codec = new Elasticsearch92Lucene103Codec(Zstd814StoredFieldsFormat.Mode.BEST_COMPRESSION);

    @Override
    protected Codec getCodec() {
        return codec;
    }
}
