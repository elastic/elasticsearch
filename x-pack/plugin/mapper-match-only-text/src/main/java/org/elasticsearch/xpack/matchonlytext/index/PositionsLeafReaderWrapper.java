/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.matchonlytext.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.elasticsearch.common.CheckedIntFunction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public final class PositionsLeafReaderWrapper extends FilterLeafReader {

    private final String field;
    private final Function<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> valueFetcherProvider;
    private final Analyzer indexAnalyzer;

    public PositionsLeafReaderWrapper(LeafReader in, String field, Function<LeafReaderContext, CheckedIntFunction<List<Object>, IOException>> valueFetcherProvider, Analyzer indexAnalyzer) {
        super(in);
        this.field = field;
        this.valueFetcherProvider = valueFetcherProvider;
        this.indexAnalyzer = indexAnalyzer;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
        return null;
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
        return null;
    }

    @Override
    public FieldInfos getFieldInfos() {
      List<FieldInfo> infos = new ArrayList<>();
      for (FieldInfo info : super.getFieldInfos()) {
          if (info.name.equals(field) == false || info.getIndexOptions() == IndexOptions.NONE) {
              infos.add(info);
              continue;
          }
          FieldInfo newInfo = new FieldInfo(info.name, info.number, info.hasVectors(), info.omitsNorms(), info.hasPayloads(), IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS, info.getDocValuesType(), info.getDocValuesGen(), info.attributes(), info.getPointDimensionCount(), info.getPointIndexDimensionCount(), info.getPointNumBytes(), info.isSoftDeletesField());
          infos.add(newInfo);
      }
      return new FieldInfos(infos.toArray(FieldInfo[]::new));
    }

    @Override
    public Terms terms(String field) throws IOException {
        if (this.field.equals(field) == false) {
            return in.terms(field);
        }
        final Terms in = super.terms(field);
        return new Terms() {

            @Override
            public TermsEnum iterator() throws IOException {

            }

            @Override
            public long size() throws IOException {
                return in.size();
            }

            @Override
            public long getSumTotalTermFreq() throws IOException {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public long getSumDocFreq() throws IOException {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public int getDocCount() throws IOException {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public boolean hasFreqs() {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public boolean hasOffsets() {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public boolean hasPositions() {
                // TODO Auto-generated method stub
                return false;
            }

            @Override
            public boolean hasPayloads() {
                // TODO Auto-generated method stub
                return false;
            }
            
        };
    }
}
