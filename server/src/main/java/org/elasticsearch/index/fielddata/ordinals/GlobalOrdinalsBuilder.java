/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.ordinals;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.OrdinalMap;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.fielddata.IndexOrdinalsFieldData;
import org.elasticsearch.index.fielddata.LeafOrdinalsFieldData;
import org.elasticsearch.index.fielddata.plain.AbstractLeafOrdinalsFieldData;
import org.elasticsearch.script.field.ToScriptFieldFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Utility class to build global ordinals.
 */
public enum GlobalOrdinalsBuilder {
    ;

    /**
     * Build global ordinals for the provided {@link IndexReader}.
     */
    public static IndexOrdinalsFieldData build(
        final IndexReader indexReader,
        IndexOrdinalsFieldData indexFieldData,
        CircuitBreaker breaker,
        Logger logger,
        ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory
    ) throws IOException {
        assert indexReader.leaves().size() > 1;
        long startTimeNS = System.nanoTime();

        final LeafOrdinalsFieldData[] atomicFD = new LeafOrdinalsFieldData[indexReader.leaves().size()];
        final SortedSetDocValues[] subs = new SortedSetDocValues[indexReader.leaves().size()];
        for (int i = 0; i < indexReader.leaves().size(); ++i) {
            atomicFD[i] = indexFieldData.load(indexReader.leaves().get(i));
            subs[i] = atomicFD[i].getOrdinalsValues();
        }
        final TermsEnum[] termsEnums = new TermsEnum[subs.length];
        final long[] weights = new long[subs.length];
        // we assume that TermsEnum are visited sequentially, so we can share the counter between them
        final long[] counter = new long[1];
        for (int i = 0; i < subs.length; ++i) {
            termsEnums[i] = new FilterLeafReader.FilterTermsEnum(subs[i].termsEnum()) {
                @Override
                public BytesRef next() throws IOException {
                    // check parent circuit breaker every 65536 calls
                    if ((counter[0]++ & 0xFFFF) == 0) {
                        breaker.addEstimateBytesAndMaybeBreak(0L, "Global Ordinals");
                    }
                    return in.next();
                }
            };
            weights[i] = subs[i].getValueCount();
        }
        final OrdinalMap ordinalMap = OrdinalMap.build(null, termsEnums, weights, PackedInts.DEFAULT);
        final long memorySizeInBytes = ordinalMap.ramBytesUsed();
        breaker.addWithoutBreaking(memorySizeInBytes);

        TimeValue took = new TimeValue(System.nanoTime() - startTimeNS, TimeUnit.NANOSECONDS);
        if (logger.isDebugEnabled()) {
            logger.debug("global-ordinals [{}][{}] took [{}]", indexFieldData.getFieldName(), ordinalMap.getValueCount(), took);
        }
        return new GlobalOrdinalsIndexFieldData(
            indexFieldData.getFieldName(),
            indexFieldData.getValuesSourceType(),
            atomicFD,
            ordinalMap,
            memorySizeInBytes,
            toScriptFieldFactory,
            took
        );
    }

    public static IndexOrdinalsFieldData buildEmpty(
        IndexReader indexReader,
        IndexOrdinalsFieldData indexFieldData,
        ToScriptFieldFactory<SortedSetDocValues> toScriptFieldFactory
    ) throws IOException {
        assert indexReader.leaves().size() > 1;
        long startTimeNS = System.nanoTime();

        final LeafOrdinalsFieldData[] atomicFD = new LeafOrdinalsFieldData[indexReader.leaves().size()];
        final SortedSetDocValues[] subs = new SortedSetDocValues[indexReader.leaves().size()];
        for (int i = 0; i < indexReader.leaves().size(); ++i) {
            atomicFD[i] = new AbstractLeafOrdinalsFieldData(toScriptFieldFactory) {
                @Override
                public SortedSetDocValues getOrdinalsValues() {
                    return DocValues.emptySortedSet();
                }

                @Override
                public long ramBytesUsed() {
                    return 0;
                }

                @Override
                public void close() {}
            };
            subs[i] = atomicFD[i].getOrdinalsValues();
        }
        final OrdinalMap ordinalMap = OrdinalMap.build(null, subs, PackedInts.DEFAULT);
        TimeValue took = new TimeValue(System.nanoTime() - startTimeNS, TimeUnit.NANOSECONDS);
        return new GlobalOrdinalsIndexFieldData(
            indexFieldData.getFieldName(),
            indexFieldData.getValuesSourceType(),
            atomicFD,
            ordinalMap,
            0,
            toScriptFieldFactory,
            took
        );
    }
}
