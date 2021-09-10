/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.oldcodecs;

import org.apache.lucene5_shaded.codecs.lucene41.Lucene41Codec;

public class WrappedLucene41Codec extends Lucene5IndexEventListener.Lucene5CodecWrapper {
    public WrappedLucene41Codec() {
        super(new Lucene41Codec());
    }
}
