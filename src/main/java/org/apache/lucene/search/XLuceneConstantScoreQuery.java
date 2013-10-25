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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

/**
 * Extension of {@link ConstantScoreQuery} that works around LUCENE-5307.
 */
// we extend CSQ so that highlighters know how to deal with this query
public class XLuceneConstantScoreQuery extends ConstantScoreQuery {

    static {
        assert Version.LUCENE_45.onOrAfter(Lucene.VERSION) : "Lucene 4.6 CSQ is fixed, remove this one!";
    }

    public XLuceneConstantScoreQuery(Query filter) {
        super(filter);
    }

    public XLuceneConstantScoreQuery(Filter filter) {
        super(filter);
    }

    @Override
    public Query rewrite(IndexReader reader) throws IOException {
        if (query != null) {
            Query rewritten = query.rewrite(reader);
            if (rewritten != query) {
                rewritten = new XLuceneConstantScoreQuery(rewritten);
                rewritten.setBoost(this.getBoost());
                return rewritten;
            }
        } else {
            assert filter != null;
            // Fix outdated usage pattern from Lucene 2.x/early-3.x:
            // because ConstantScoreQuery only accepted filters,
            // QueryWrapperFilter was used to wrap queries.
            if (filter instanceof QueryWrapperFilter) {
                final QueryWrapperFilter qwf = (QueryWrapperFilter) filter;
                final Query rewritten = new XLuceneConstantScoreQuery(qwf.getQuery().rewrite(reader));
                rewritten.setBoost(this.getBoost());
                return rewritten;
            }
        }
        return this;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher) throws IOException {
        return new XConstantWeight(searcher);
    }

    protected class XConstantWeight extends Weight {
        private final Weight innerWeight;
        private float queryNorm;
        private float queryWeight;

        public XConstantWeight(IndexSearcher searcher) throws IOException {
            this.innerWeight = (query == null) ? null : query.createWeight(searcher);
        }

        @Override
        public Query getQuery() {
            return XLuceneConstantScoreQuery.this;
        }

        @Override
        public float getValueForNormalization() throws IOException {
            // we calculate sumOfSquaredWeights of the inner weight, but ignore it (just to initialize everything)
            if (innerWeight != null) innerWeight.getValueForNormalization();
            queryWeight = getBoost();
            return queryWeight * queryWeight;
        }

        @Override
        public void normalize(float norm, float topLevelBoost) {
            this.queryNorm = norm * topLevelBoost;
            queryWeight *= this.queryNorm;
            // we normalize the inner weight, but ignore it (just to initialize everything)
            if (innerWeight != null) innerWeight.normalize(norm, topLevelBoost);
        }

        @Override
        public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder,
                boolean topScorer, final Bits acceptDocs) throws IOException {
            final DocIdSetIterator disi;
            if (filter != null) {
                assert query == null;
                final DocIdSet dis = filter.getDocIdSet(context, acceptDocs);
                if (dis == null) {
                    return null;
                }
                disi = dis.iterator();
            } else {
                assert query != null && innerWeight != null;
                disi = innerWeight.scorer(context, scoreDocsInOrder, topScorer, acceptDocs);
            }

            if (disi == null) {
                return null;
            }
            return new XConstantScorer(disi, this, queryWeight);
        }

        @Override
        public boolean scoresDocsOutOfOrder() {
            return (innerWeight != null) ? innerWeight.scoresDocsOutOfOrder() : false;
        }

        @Override
        public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
            final Scorer cs = scorer(context, true, false, context.reader().getLiveDocs());
            final boolean exists = (cs != null && cs.advance(doc) == doc);

            final ComplexExplanation result = new ComplexExplanation();
            if (exists) {
                result.setDescription(XLuceneConstantScoreQuery.this.toString() + ", product of:");
                result.setValue(queryWeight);
                result.setMatch(Boolean.TRUE);
                result.addDetail(new Explanation(getBoost(), "boost"));
                result.addDetail(new Explanation(queryNorm, "queryNorm"));
            } else {
                result.setDescription(XLuceneConstantScoreQuery.this.toString() + " doesn't match id " + doc);
                result.setValue(0);
                result.setMatch(Boolean.FALSE);
            }
            return result;
        }
    }

    protected class XConstantScorer extends Scorer {
        final DocIdSetIterator docIdSetIterator;
        final float theScore;

        public XConstantScorer(DocIdSetIterator docIdSetIterator, Weight w, float theScore) {
            super(w);
            this.theScore = theScore;
            this.docIdSetIterator = docIdSetIterator;
        }

        @Override
        public int nextDoc() throws IOException {
            return docIdSetIterator.nextDoc();
        }

        @Override
        public int docID() {
            return docIdSetIterator.docID();
        }

        @Override
        public float score() throws IOException {
            assert docIdSetIterator.docID() != NO_MORE_DOCS;
            return theScore;
        }

        @Override
        public int freq() throws IOException {
            return 1;
        }

        @Override
        public int advance(int target) throws IOException {
            return docIdSetIterator.advance(target);
        }

        @Override
        public long cost() {
            return docIdSetIterator.cost();
        }

        private Collector wrapCollector(final Collector collector) {
            return new Collector() {
                @Override
                public void setScorer(Scorer scorer) throws IOException {
                    // we must wrap again here, but using the scorer passed in as parameter:
                    collector.setScorer(new ConstantScorer(scorer, XConstantScorer.this.weight, XConstantScorer.this.theScore));
                }

                @Override
                public void collect(int doc) throws IOException {
                    collector.collect(doc);
                }

                @Override
                public void setNextReader(AtomicReaderContext context) throws IOException {
                    collector.setNextReader(context);
                }

                @Override
                public boolean acceptsDocsOutOfOrder() {
                    return collector.acceptsDocsOutOfOrder();
                }
            };
        }

        // this optimization allows out of order scoring as top scorer!
        @Override
        public void score(Collector collector) throws IOException {
            if (query != null) {
                ((Scorer) docIdSetIterator).score(wrapCollector(collector));
            } else {
                super.score(collector);
            }
        }

        // this optimization allows out of order scoring as top scorer,
        @Override
        public boolean score(Collector collector, int max, int firstDocID) throws IOException {
            if (query != null) {
                return ((Scorer) docIdSetIterator).score(wrapCollector(collector), max, firstDocID);
            } else {
                return super.score(collector, max, firstDocID);
            }
        }

        @Override
        public Collection<ChildScorer> getChildren() {
            if (query != null)
                return Collections.singletonList(new ChildScorer((Scorer) docIdSetIterator, "constant"));
            else
                return Collections.emptyList();
        }
    }

}
