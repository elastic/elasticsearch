/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.mapper.IdFieldMapper;

import java.io.Closeable;
import java.io.IOException;

/**
 * Owns the {@link DocValuesProducer} dedicated to bloom-filter lookups and vends per-caller
 * {@link BloomFilter} instances from it.
 *
 * <p>Thread-safety contract: {@link DocValuesProducer#getBinary} returns a new, independent
 * {@link org.apache.lucene.index.BinaryDocValues} reader on every call, each backed by its own
 * I/O buffer. Callers must therefore invoke {@link #createBloomFilterInstance()} once per logical
 * lookup context (e.g. once per {@link org.apache.lucene.index.TermsEnum} obtained via
 * {@link org.apache.lucene.index.Terms#iterator()}) and must not share the returned
 * {@link BloomFilter} across threads. Sharing a single {@link BloomFilter} across threads would
 * race on the mutable buffer state of its underlying
 * {@link org.apache.lucene.store.RandomAccessInput}.
 */
public class IdBloomFilterSupplier implements Closeable {
    private final DocValuesProducer docValuesProducer;
    private final FieldInfo idFieldInfo;

    public IdBloomFilterSupplier(SegmentReadState state, DocValuesProducer docValuesProducer) {
        var idFieldInfo = state.fieldInfos.fieldInfo(IdFieldMapper.NAME);
        if (idFieldInfo == null) {
            throw new IllegalStateException(IdFieldMapper.NAME + " field not found");
        }
        if (idFieldInfo.getDocValuesType() != DocValuesType.BINARY) {
            throw new IllegalStateException(IdFieldMapper.NAME + " bloom filter field must be of type binary");
        }

        this.idFieldInfo = idFieldInfo;
        this.docValuesProducer = docValuesProducer;
    }

    /**
     * Returns a {@link BloomFilter} for the {@code _id} field backed by a freshly obtained
     * reader from the underlying {@link DocValuesProducer}. Must be called once per lookup
     * context; the returned instance must not be shared across threads.
     */
    public BloomFilter createBloomFilterInstance() throws IOException {
        var binaryDocValuesProducer = docValuesProducer.getBinary(idFieldInfo);
        if (binaryDocValuesProducer instanceof BloomFilter bloomFilter) {
            return bloomFilter;
        } else {
            return BloomFilter.NO_FILTER;
        }
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(docValuesProducer);
    }
}
