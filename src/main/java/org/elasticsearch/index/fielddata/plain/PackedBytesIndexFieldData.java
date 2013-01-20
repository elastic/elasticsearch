/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.fielddata.plain;

import org.apache.lucene.index.*;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PagedBytes;
import org.apache.lucene.util.packed.GrowableWriter;
import org.apache.lucene.util.packed.PackedInts;
import org.elasticsearch.ElasticSearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.ordinals.EmptyOrdinals;
import org.elasticsearch.index.fielddata.ordinals.MultiFlatArrayOrdinals;
import org.elasticsearch.index.fielddata.ordinals.SingleArrayOrdinals;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.index.settings.IndexSettings;

import java.util.ArrayList;

/**
 */
public class PackedBytesIndexFieldData extends AbstractIndexFieldData<PackedBytesAtomicFieldData> implements IndexOrdinalFieldData<PackedBytesAtomicFieldData> {

    public static class Builder implements IndexFieldData.Builder {

        @Override
        public IndexFieldData build(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType type, IndexFieldDataCache cache) {
            return new PackedBytesIndexFieldData(index, indexSettings, fieldNames, type, cache);
        }
    }

    public PackedBytesIndexFieldData(Index index, @IndexSettings Settings indexSettings, FieldMapper.Names fieldNames, FieldDataType fieldDataType, IndexFieldDataCache cache) {
        super(index, indexSettings, fieldNames, fieldDataType, cache);
    }

    @Override
    public boolean valuesOrdered() {
        return true;
    }

    @Override
    public PackedBytesAtomicFieldData load(AtomicReaderContext context) {
        try {
            return cache.load(context, this);
        } catch (Throwable e) {
            if (e instanceof ElasticSearchException) {
                throw (ElasticSearchException) e;
            } else {
                throw new ElasticSearchException(e.getMessage(), e);
            }
        }
    }

    @Override
    public PackedBytesAtomicFieldData loadDirect(AtomicReaderContext context) throws Exception {
        AtomicReader reader = context.reader();

        Terms terms = reader.terms(getFieldNames().indexName());
        if (terms == null) {
            final PagedBytes bytes = new PagedBytes(1);
            // 0 is reserved for "unset"
            bytes.copyUsingLengthPrefix(new BytesRef());
            GrowableWriter termOrdToBytesOffset = new GrowableWriter(1, 2, PackedInts.FASTEST);
            return new PackedBytesAtomicFieldData(bytes.freeze(true), termOrdToBytesOffset.getMutable(), new EmptyOrdinals(reader.maxDoc()));
        }

        final PagedBytes bytes = new PagedBytes(15);
        int startBytesBPV;
        int startTermsBPV;
        int startNumUniqueTerms;

        int maxDoc = reader.maxDoc();
        final int termCountHardLimit;
        if (maxDoc == Integer.MAX_VALUE) {
            termCountHardLimit = Integer.MAX_VALUE;
        } else {
            termCountHardLimit = maxDoc + 1;
        }

        // Try for coarse estimate for number of bits; this
        // should be an underestimate most of the time, which
        // is fine -- GrowableWriter will reallocate as needed
        long numUniqueTerms = terms.size();
        if (numUniqueTerms != -1L) {
            if (numUniqueTerms > termCountHardLimit) {
                // app is misusing the API (there is more than
                // one term per doc); in this case we make best
                // effort to load what we can (see LUCENE-2142)
                numUniqueTerms = termCountHardLimit;
            }

            startBytesBPV = PackedInts.bitsRequired(numUniqueTerms * 4);
            startTermsBPV = PackedInts.bitsRequired(numUniqueTerms);

            startNumUniqueTerms = (int) numUniqueTerms;
        } else {
            startBytesBPV = 1;
            startTermsBPV = 1;
            startNumUniqueTerms = 1;
        }

        // TODO: expose this as an option..., have a nice parser for it...
        float acceptableOverheadRatio = PackedInts.FAST;

        GrowableWriter termOrdToBytesOffset = new GrowableWriter(startBytesBPV, 1 + startNumUniqueTerms, acceptableOverheadRatio);

        ArrayList<int[]> ordinals = new ArrayList<int[]>();
        int[] idx = new int[reader.maxDoc()];
        ordinals.add(new int[reader.maxDoc()]);

        // 0 is reserved for "unset"
        bytes.copyUsingLengthPrefix(new BytesRef());
        int termOrd = 1;

        TermsEnum termsEnum = terms.iterator(null);
        try

        {
            DocsEnum docsEnum = null;
            for (BytesRef term = termsEnum.next(); term != null; term = termsEnum.next()) {
                if (termOrd == termOrdToBytesOffset.size()) {
                    // NOTE: this code only runs if the incoming
                    // reader impl doesn't implement
                    // size (which should be uncommon)
                    termOrdToBytesOffset = termOrdToBytesOffset.resize(ArrayUtil.oversize(1 + termOrd, 1));
                }
                termOrdToBytesOffset.set(termOrd, bytes.copyUsingLengthPrefix(term));

                docsEnum = termsEnum.docs(reader.getLiveDocs(), docsEnum, 0);
                for (int docId = docsEnum.nextDoc(); docId != DocsEnum.NO_MORE_DOCS; docId = docsEnum.nextDoc()) {
                    int[] ordinal;
                    if (idx[docId] >= ordinals.size()) {
                        ordinal = new int[reader.maxDoc()];
                        ordinals.add(ordinal);
                    } else {
                        ordinal = ordinals.get(idx[docId]);
                    }
                    ordinal[docId] = termOrd;
                    idx[docId]++;
                }
                termOrd++;
            }
        } catch (RuntimeException e) {
            if (e.getClass().getName().endsWith("StopFillCacheException")) {
                // all is well, in case numeric parsers are used.
            } else {
                throw e;
            }
        }

        PagedBytes.Reader bytesReader = bytes.freeze(true);
        PackedInts.Reader termOrdToBytesOffsetReader = termOrdToBytesOffset.getMutable();

        if (ordinals.size() == 1) {
            return new PackedBytesAtomicFieldData(bytesReader, termOrdToBytesOffsetReader, new SingleArrayOrdinals(ordinals.get(0), termOrd));
        } else {
            int[][] nativeOrdinals = new int[ordinals.size()][];
            for (int i = 0; i < nativeOrdinals.length; i++) {
                nativeOrdinals[i] = ordinals.get(i);
            }
            return new PackedBytesAtomicFieldData(bytesReader, termOrdToBytesOffsetReader, new MultiFlatArrayOrdinals(nativeOrdinals, termOrd));
        }

    }

    @Override
    public XFieldComparatorSource comparatorSource(@Nullable Object missingValue) {
        // TODO support "missingValue" for sortMissingValue options here...
        return new BytesRefFieldComparatorSource(this);
    }
}
