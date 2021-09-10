/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.oldcodecs;

import org.apache.lucene5_shaded.codecs.lucene54.Lucene54Codec;

public class WrappedLucene54Codec extends Lucene5IndexEventListener.Lucene5CodecWrapper {
    public WrappedLucene54Codec() {
        super(new Lucene54Codec());
    }
}
