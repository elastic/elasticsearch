/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.index.BaseTermsEnum;
import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.io.UncheckedIOException;

public abstract class LazyFilterTermsEnum extends BaseTermsEnum {
    protected abstract TermsEnum getDelegate() throws IOException;

    @Override
    public SeekStatus seekCeil(BytesRef text) throws IOException {
        return getDelegate().seekCeil(text);
    }

    @Override
    public void seekExact(long ord) throws IOException {
        getDelegate().seekExact(ord);
    }

    @Override
    public BytesRef term() throws IOException {
        return getDelegate().term();
    }

    @Override
    public long ord() throws IOException {
        return getDelegate().ord();
    }

    @Override
    public int docFreq() throws IOException {
        return getDelegate().docFreq();
    }

    @Override
    public long totalTermFreq() throws IOException {
        return getDelegate().totalTermFreq();
    }

    @Override
    public PostingsEnum postings(PostingsEnum reuse, int flags) throws IOException {
        return getDelegate().postings(reuse, flags);
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
        return getDelegate().impacts(flags);
    }

    @Override
    public BytesRef next() throws IOException {
        return getDelegate().next();
    }

    @Override
    public AttributeSource attributes() {
        try {
            return getDelegate().attributes();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public static TermsEnum unwrap(TermsEnum wrapped) throws IOException {
        if (wrapped instanceof LazyFilterTermsEnum wrappedEnum) {
            return wrappedEnum.getDelegate();
        }
        return wrapped;
    }
}
