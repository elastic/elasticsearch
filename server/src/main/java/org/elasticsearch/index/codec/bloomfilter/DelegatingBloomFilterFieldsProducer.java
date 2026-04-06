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
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.LongAdder;

/**
 * A FieldsProducer that uses a Bloom filter for fast term existence checks before
 * delegating exact lookups to the underlying FieldsProducer. This avoids a potentially
 * expensive seek operations for non-existent terms.
 */
public class DelegatingBloomFilterFieldsProducer extends FieldsProducer {
    private static final Set<String> FIELD_NAMES = Set.of(IdFieldMapper.NAME);
    private static final Logger logger = LogManager.getLogger(DelegatingBloomFilterFieldsProducer.class);

    private static final LongAdder NO_OP = new LongAdder() {
        @Override
        public void increment() {}

        @Override
        public void add(long x) {}

        @Override
        public long sum() {
            return 0L;
        }
    };

    private final FieldsProducer delegate;
    private final BloomFilter bloomFilter;
    private final LongAdder checks;
    private final LongAdder falsePositives;

    public DelegatingBloomFilterFieldsProducer(FieldsProducer delegate, BloomFilter bloomFilter) {
        this.delegate = delegate;
        this.bloomFilter = bloomFilter;
        boolean trace = logger.isTraceEnabled();
        this.checks = trace ? new LongAdder() : NO_OP;
        this.falsePositives = trace ? new LongAdder() : NO_OP;
    }

    @Override
    public void close() throws IOException {
        if (logger.isTraceEnabled() && checks != NO_OP) {
            long totalChecks = checks.sum();
            long totalFalsePositives = falsePositives.sum();
            logger.trace(
                "bloom filter [{}]: saturation={} checks={} false_positives={} fpr={}",
                bloomFilter,
                String.format(Locale.ROOT, "%.2f%%", bloomFilter.saturation() * 100),
                totalChecks,
                totalFalsePositives,
                totalChecks > 0 ? String.format(Locale.ROOT, "%.2e", (double) totalFalsePositives / totalChecks) : "n/a"
            );
        }
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
                        checks.increment();
                        if (bloomFilter.mayContainValue(field, text) == false) {
                            return false;
                        }
                        var exists = getDelegate().seekExact(text);
                        if (exists == false) {
                            falsePositives.increment();
                        }
                        return exists;
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
