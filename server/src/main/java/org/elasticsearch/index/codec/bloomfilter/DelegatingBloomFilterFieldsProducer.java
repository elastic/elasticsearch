/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;

/**
 * A FieldsProducer that uses a Bloom filter for fast term existence checks before
 * delegating exact lookups to the underlying FieldsProducer. This avoids a potentially
 * expensive seek operations for non-existent terms.
 */
public class DelegatingBloomFilterFieldsProducer extends FieldsProducer {
    private static final Set<String> FIELD_NAMES = Set.of(IdFieldMapper.NAME);
    private final FieldsProducer delegate;
    private final BloomFilter bloomFilter;

    public DelegatingBloomFilterFieldsProducer(FieldsProducer delegate, BloomFilter bloomFilter) {
        this.delegate = delegate;
        this.bloomFilter = bloomFilter;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(delegate, bloomFilter);
    }

    @Override
    public void checkIntegrity() throws IOException {
        delegate.checkIntegrity();
    }

    @Override
    public Iterator<String> iterator() {
        return delegate.iterator();
    }

    @Override
    public Terms terms(String field) throws IOException {
        assert FIELD_NAMES.contains(field) : "Expected one of " + FIELD_NAMES + " but got " + field;
        final Terms terms = delegate.terms(field);
        return new FilterLeafReader.FilterTerms(terms) {
            @Override
            public TermsEnum iterator() throws IOException {
                return new LazyFilterTermsEnum() {
                    private TermsEnum termsEnum;

                    @Override
                    protected TermsEnum getDelegate() throws IOException {
                        if (termsEnum == null) {
                            termsEnum = terms.iterator();
                        }
                        return termsEnum;
                    }

                    @Override
                    public boolean seekExact(BytesRef text) throws IOException {
                        if (bloomFilter.mayContainTerm(field, text) == false) {
                            return false;
                        }
                        return getDelegate().seekExact(text);
                    }
                };
            }
        };
    }

    @Override
    public int size() {
        return delegate.size();
    }
}
