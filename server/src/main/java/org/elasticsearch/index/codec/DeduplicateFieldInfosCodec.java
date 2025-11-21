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
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdCodec;

public sealed class DeduplicateFieldInfosCodec extends FilterCodec permits TSDBSyntheticIdCodec {

    private final DeduplicatingFieldInfosFormat fieldInfosFormat;

    @SuppressWarnings("this-escape")
    protected DeduplicateFieldInfosCodec(Codec delegate) {
        super(delegate.getName(), delegate);
        this.fieldInfosFormat = createFieldInfosFormat(delegate.fieldInfosFormat());
    }

    protected DeduplicatingFieldInfosFormat createFieldInfosFormat(FieldInfosFormat delegate) {
        return new DeduplicatingFieldInfosFormat(delegate);
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    public Codec delegate() {
        return delegate;
    }

}
