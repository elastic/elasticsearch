package org.apache.lucene.search;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Terms;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.XGeoUtils;

/**
 * Custom ConstantScoreWrapper for {@code GeoPointTermQuery} that cuts over to DocValues
 * for post filtering boundary ranges. Multi-valued GeoPoint documents are supported.
 *
 * @lucene.experimental
 */
final class XGeoPointTermQueryConstantScoreWrapper<Q extends XGeoPointTermQuery> extends Query {
  protected final Q query;

  protected XGeoPointTermQueryConstantScoreWrapper(Q query) {
    this.query = query;
  }

  @Override
  public String toString(String field) {
    return query.toString();
  }

  @Override
  public final boolean equals(final Object o) {
    if (super.equals(o) == false) {
      return false;
    }
    final XGeoPointTermQueryConstantScoreWrapper<?> that = (XGeoPointTermQueryConstantScoreWrapper<?>) o;
    return this.query.equals(that.query) && this.getBoost() == that.getBoost();
  }

  @Override
  public final int hashCode() {
    return 31 * super.hashCode() + query.hashCode();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, boolean needsScores) throws IOException {
    return new ConstantScoreWeight(this) {

      private DocIdSet getDocIDs(LeafReaderContext context) throws IOException {
        final Terms terms = context.reader().terms(query.field);
        if (terms == null) {
          return DocIdSet.EMPTY;
        }

        final XGeoPointTermsEnum termsEnum = (XGeoPointTermsEnum)(query.getTermsEnum(terms));
        assert termsEnum != null;

        LeafReader reader = context.reader();
        DocIdSetBuilder builder = new DocIdSetBuilder(reader.maxDoc());
        PostingsEnum docs = null;
        SortedNumericDocValues sdv = reader.getSortedNumericDocValues(query.field);

        while (termsEnum.next() != null) {
          docs = termsEnum.postings(docs, PostingsEnum.NONE);
          // boundary terms need post filtering by
          if (termsEnum.boundaryTerm()) {
            int docId = docs.nextDoc();
            do {
              sdv.setDocument(docId);
              for (int i=0; i<sdv.count(); ++i) {
                final long hash = sdv.valueAt(i);
                final double lon = XGeoUtils.mortonUnhashLon(hash);
                final double lat = XGeoUtils.mortonUnhashLat(hash);
                if (termsEnum.postFilter(lon, lat)) {
                  builder.add(docId);
                }
              }
            } while ((docId = docs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS);
          } else {
            builder.add(docs);
          }
        }

        return builder.build();
      }

      private Scorer scorer(DocIdSet set) throws IOException {
        if (set == null) {
          return null;
        }
        final DocIdSetIterator disi = set.iterator();
        if (disi == null) {
          return null;
        }
        return new ConstantScoreScorer(this, score(), disi);
      }

      @Override
      public BulkScorer bulkScorer(LeafReaderContext context) throws IOException {
        final Scorer scorer = scorer(getDocIDs(context));
        if (scorer == null) {
          return null;
        }
        return new DefaultBulkScorer(scorer);
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        return scorer(getDocIDs(context));
      }
    };
  }
}
