/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.oldcodecs;

import org.apache.lucene5_shaded.codecs.lucene410.Lucene410Codec;

public class WrappedLucene410Codec extends Lucene5IndexEventListener.Lucene5CodecWrapper {
    public WrappedLucene410Codec() {
        super(new Lucene410Codec());
    }
}
