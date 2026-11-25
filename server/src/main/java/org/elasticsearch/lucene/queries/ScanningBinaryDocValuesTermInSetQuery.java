/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.queries;

import org.apache.lucene.index.PrefixCodedTerms;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.BytesRefComparator;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.StringSorter;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A query for matching any value from a set of BytesRef values for a specific field.
 * <p>
 * This implementation is slow, because it potentially scans binary doc values for each document.
 */
public final class ScanningBinaryDocValuesTermInSetQuery extends AbstractBinaryDocValuesQuery implements Accountable {

    private final PrefixCodedTerms termData;
    private final int termDataHashCode;
    private final ExactTermSet termSet;

    public ScanningBinaryDocValuesTermInSetQuery(String fieldName, List<BytesRef> terms, BinaryDocValuesFormat binaryFormat) {
        this(fieldName, packTerms(fieldName, Objects.requireNonNull(terms)), binaryFormat);
    }

    private ScanningBinaryDocValuesTermInSetQuery(String fieldName, PrefixCodedTerms termData, BinaryDocValuesFormat binaryFormat) {
        this(fieldName, termData, new ExactTermSet(termData), binaryFormat);
    }

    private ScanningBinaryDocValuesTermInSetQuery(
        String fieldName,
        PrefixCodedTerms termData,
        ExactTermSet termSet,
        BinaryDocValuesFormat binaryFormat
    ) {
        super(fieldName, termSet::contains, binaryFormat);
        this.termData = termData;
        this.termDataHashCode = termData.hashCode();
        this.termSet = termSet;
    }

    @Override
    public long ramBytesUsed() {
        return RamUsageEstimator.shallowSizeOf(this) + termSet.ramBytesUsed() + termData.ramBytesUsed();
    }

    private static PrefixCodedTerms packTerms(String field, List<BytesRef> terms) {
        BytesRef[] sortedTerms = terms.toArray(new BytesRef[0]);
        new StringSorter(BytesRefComparator.NATURAL) {

            @Override
            protected void get(BytesRefBuilder builder, BytesRef result, int i) {
                BytesRef term = sortedTerms[i];
                result.length = term.length;
                result.offset = term.offset;
                result.bytes = term.bytes;
            }

            @Override
            protected void swap(int i, int j) {
                BytesRef tmp = sortedTerms[i];
                sortedTerms[i] = sortedTerms[j];
                sortedTerms[j] = tmp;
            }
        }.sort(0, sortedTerms.length);
        PrefixCodedTerms.Builder builder = new PrefixCodedTerms.Builder();
        BytesRefBuilder previous = null;
        for (BytesRef term : sortedTerms) {
            if (previous == null) {
                previous = new BytesRefBuilder();
            } else if (previous.get().equals(term)) {
                continue; // deduplicate
            }
            builder.add(field, term);
            previous.copyBytes(term);
        }
        return builder.finish();
    }

    @Override
    protected float matchCost() {
        // the cost for single term matching is 10, a hash lookup is O(term length)
        // so this is only slightly slower than single term
        return 11;
    }

    @Override
    public String toString(String field) {
        StringBuilder sb = new StringBuilder("ScanningBinaryDocValuesTermInSetQuery(fieldName=");
        sb.append(field).append(",terms=[");
        PrefixCodedTerms.TermIterator iterator = termData.iterator();
        boolean first = true;
        for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
            if (first == false) {
                sb.append(",");
            }
            sb.append(Term.toString(term));
            first = false;
        }
        sb.append("])");
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (sameClassAs(o) == false) {
            return false;
        }
        ScanningBinaryDocValuesTermInSetQuery that = (ScanningBinaryDocValuesTermInSetQuery) o;
        return Objects.equals(fieldName, that.fieldName)
            && termDataHashCode == that.termDataHashCode
            && termData.equals(that.termData)
            && binaryFormat == that.binaryFormat;
    }

    @Override
    public int hashCode() {
        return Objects.hash(classHash(), fieldName, termDataHashCode, binaryFormat);
    }

    private static final class ExactTermSet implements Accountable {
        private final byte[] bytes;
        private final int[] offsets;
        private final int[] table;
        private final int mask;

        ExactTermSet(PrefixCodedTerms termData) {
            int size = (int) termData.size();
            int capacity = Integer.highestOneBit(Math.max(size * 2, 2) - 1) << 1;
            this.mask = capacity - 1;
            this.table = new int[capacity];
            Arrays.fill(table, -1);
            this.offsets = new int[size + 1];
            BytesRefBuilder all = new BytesRefBuilder();
            PrefixCodedTerms.TermIterator iterator = termData.iterator();
            int ord = 0;
            for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
                offsets[ord] = all.length();
                all.append(term);
                ord++;
            }
            offsets[size] = all.length();
            this.bytes = Arrays.copyOf(all.bytes(), all.length());
            for (ord = 0; ord < size; ord++) {
                int slot = StringHelper.murmurhash3_x86_32(bytes, offsets[ord], offsets[ord + 1] - offsets[ord], 0) & mask;
                while (table[slot] != -1) {
                    slot = (slot + 1) & mask;
                }
                table[slot] = ord;
            }
        }

        boolean contains(BytesRef term) {
            int slot = StringHelper.murmurhash3_x86_32(term.bytes, term.offset, term.length, 0) & mask;
            for (int ord = table[slot]; ord != -1; ord = table[slot]) {
                if (Arrays.equals(bytes, offsets[ord], offsets[ord + 1], term.bytes, term.offset, term.offset + term.length)) {
                    return true;
                }
                slot = (slot + 1) & mask;
            }
            return false;
        }

        @Override
        public long ramBytesUsed() {
            return RamUsageEstimator.shallowSizeOf(this) + RamUsageEstimator.sizeOf(bytes) + RamUsageEstimator.sizeOf(offsets)
                + RamUsageEstimator.sizeOf(table);
        }
    }
}
