/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.oldcodecs;

import org.apache.lucene5_shaded.codecs.lucene49.Lucene49Codec;

public class WrappedLucene49Codec extends Lucene5IndexEventListener.Lucene5CodecWrapper {
    public WrappedLucene49Codec() {
        super(new Lucene49Codec());
    }
}
