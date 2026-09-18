/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1";
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.tests.index.BaseStoredFieldsFormatTestCase;

public class Elasticsearch96BestCompressionStoredFieldsFormatTests extends BaseStoredFieldsFormatTestCase {

    private final Codec codec = new Elasticsearch96Codec(Lucene104Codec.Mode.BEST_COMPRESSION);

    @Override
    protected Codec getCodec() {
        return codec;
    }
}
