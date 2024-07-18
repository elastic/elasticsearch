/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;

public class FilterCodecService implements CodecSupplier {

    private final CodecSupplier in;

    public FilterCodecService(CodecSupplier in) {
        this.in = in;
    }

    @Override
    public Codec codec(String name) {
        return in.codec(name);
    }
}
