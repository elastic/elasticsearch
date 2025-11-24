/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.elasticsearch.common.util.BigArrays;

public class ES93TSDBZSTDCompressionLucene103Codec extends TSDBCodecWithSyntheticId {
    /** Public no-arg constructor, needed for SPI loading at read-time. */
    public ES93TSDBZSTDCompressionLucene103Codec() {
        this(new Elasticsearch92Lucene103Codec(), null, true);
    }

    ES93TSDBZSTDCompressionLucene103Codec(Elasticsearch92Lucene103Codec delegate, BigArrays bigArrays, boolean bloomFilterEnabled) {
        super("ES93TSZSTDCompressionLucene103Codec", delegate, bigArrays, bloomFilterEnabled);
    }
}
