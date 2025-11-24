/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene103.Lucene103Codec;
import org.elasticsearch.common.util.BigArrays;

public class ES93TSDBLuceneDefaultCodec extends TSDBCodecWithSyntheticId {
    /** Public no-arg constructor, needed for SPI loading at read-time. */
    public ES93TSDBLuceneDefaultCodec() {
        this(new Lucene103Codec(), null, true);
    }

    ES93TSDBLuceneDefaultCodec(Codec delegate, BigArrays bigArrays, boolean bloomFilterEnabled) {
        super("ES93TSLuceneDefaultCodec", delegate, bigArrays, bloomFilterEnabled);
    }
}
