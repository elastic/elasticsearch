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
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;

public final class DeduplicateFieldInfosCodec extends FilterCodec {

    private final DeduplicatingFieldInfosFormat deduplicatingFieldInfosFormat;

    @SuppressWarnings("this-escape")
    protected DeduplicateFieldInfosCodec(String name, Codec delegate) {
        super(name, delegate);
        this.deduplicatingFieldInfosFormat = new DeduplicatingFieldInfosFormat(super.fieldInfosFormat());
    }

    @Override
    public FieldInfosFormat fieldInfosFormat() {
        return deduplicatingFieldInfosFormat;
    }

    public Codec delegate() {
        return delegate;
    }

}
