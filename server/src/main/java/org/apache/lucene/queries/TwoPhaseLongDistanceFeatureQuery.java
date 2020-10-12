/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
package org.apache.lucene.queries;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.DocIdSetBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

public abstract class TwoPhaseLongDistanceFeatureQuery extends Query {

    private final String field;
    private final long originExact;
    private final long pivotDistanceExact;
    private final long originApprox;

    public TwoPhaseLongDistanceFeatureQuery(String field, long originExact, long pivotDistanceExact, long originApprox) {
        this.field = Objects.requireNonNull(field);
        this.originExact = originExact;
        if (pivotDistanceExact <= 0) {
            throw new IllegalArgumentException("pivotDistance must be > 0, got " + pivotDistanceExact);
        }
        this.pivotDistanceExact = pivotDistanceExact;
        this.originApprox = originApprox;
    }

    public abstract long convertDistance(long distanceExact);

    @Override
    public final boolean equals(Object o) {
        return sameClassAs(o) &&
            equalsTo(getClass().cast(o));
    }

    private boolean equalsTo(TwoPhaseLongDistanceFeatureQuery other) {
        return Objects.equals(field, other.field) &&
            originExact == other.originExact &&
            pivotDistanceExact == other.pivotDistanceExact &&
            originApprox == other.originApprox;
    }

    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + field.hashCode();
        h = 31 * h + Long.hashCode(originExact);
        h = 31 * h + Long.hashCode(pivotDistanceExact);
        h = 31 * h + Long.hashCode(originApprox);
        return h;
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(field)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String field) {
        return getClass().getSimpleName() + "(field=" + field + ",origin=" + originExact + ",pivotDistance=" + pivotDistanceExact + ")";
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return new Weight(this) {

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }

            @Override
            public void extractTerms(Set<Term> terms) {
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                SortedNumericDocValues multiDocValues = DocValues.getSortedNumeric(context.reader(), field);
                if (multiDocValues.advanceExact(doc) == false) {
                    return Explanation.noMatch("Document " + doc + " doesn't have a value for field " + field);
                }
                long value = selectValue(multiDocValues);
                long distance = Math.max(value, originExact) - Math.min(value, originExact);
                if (distance < 0) {
                    // underflow, treat as MAX_VALUE
                    distance = Long.MAX_VALUE;
                }
                float score = (float) (boost * (pivotDistanceExact / (pivotDistanceExact + (double) distance)));
                return Explanation.match(score, "Distance score, computed as weight * pivotDistance / (pivotDistance + abs(value - origin)) from:",
                    Explanation.match(boost, "weight"),
                    Explanation.match(pivotDistanceExact, "pivotDistance"),
                    Explanation.match(originExact, "origin"),
                    Explanation.match(value, "current value"));
            }

            private long selectValue(SortedNumericDocValues multiDocValues) throws IOException {
                int count = multiDocValues.docValueCount();

                long next = multiDocValues.nextValue();
                if (count == 1 || next >= originExact) {
                    return next;
                }
                long previous = next;
                for (int i = 1; i < count; ++i) {
                    next = multiDocValues.nextValue();
                    if (next >= originExact) {
                        // Unsigned comparison because of underflows
                        if (Long.compareUnsigned(originExact - previous, next - originExact) < 0) {
                            return previous;
                        } else {
                            return next;
                        }
                    }
                    previous = next;
                }

                assert next < originExact;
                return next;
            }

            private NumericDocValues selectValues(SortedNumericDocValues multiDocValues) {
                final NumericDocValues singleton = DocValues.unwrapSingleton(multiDocValues);
                if (singleton != null) {
                    return singleton;
                }
                return new NumericDocValues() {

                    long value;

                    @Override
                    public long longValue() throws IOException {
                        return value;
                    }

                    @Override
                    public boolean advanceExact(int target) throws IOException {
                        if (multiDocValues.advanceExact(target)) {
                            value = selectValue(multiDocValues);
                            return true;
                        } else {
                            return false;
                        }
                    }

                    @Override
                    public int docID() {
                        return multiDocValues.docID();
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        return multiDocValues.nextDoc();
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        return multiDocValues.advance(target);
                    }

                    @Override
                    public long cost() {
                        return multiDocValues.cost();
                    }

                };
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                PointValues pointValues = context.reader().getPointValues(field);
                if (pointValues == null) {
                    // No data on this segment
                    return null;
                }
                final SortedNumericDocValues multiDocValues = DocValues.getSortedNumeric(context.reader(), field);
                final NumericDocValues docValues = selectValues(multiDocValues);

                final Weight weight = this;
                return new ScorerSupplier() {

                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        return new DistanceScorer(weight, context.reader().maxDoc(), leadCost, boost, pointValues, docValues);
                    }

                    @Override
                    public long cost() {
                        return docValues.cost();
                    }
                };
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                ScorerSupplier scorerSupplier = scorerSupplier(context);
                if (scorerSupplier == null) {
                    return null;
                }
                return scorerSupplier.get(Long.MAX_VALUE);
            }

        };
    }

    private class DistanceScorer extends Scorer {

        private final int maxDoc;
        private DocIdSetIterator it;
        private int doc = -1;
        private final long leadCost;
        private final float boost;
        private final PointValues pointValues;
        private final NumericDocValues docValues;
        private long maxDistance = Long.MAX_VALUE;

        protected DistanceScorer(Weight weight, int maxDoc, long leadCost, float boost,
                                 PointValues pointValues, NumericDocValues docValues) {
            super(weight);
            this.maxDoc = maxDoc;
            this.leadCost = leadCost;
            this.boost = boost;
            this.pointValues = pointValues;
            this.docValues = docValues;
            // initially use doc values in order to iterate all documents that have
            // a value for this field
            this.it = docValues;
        }

        @Override
        public int docID() {
            return doc;
        }

        private float score(double distance) {
            return (float) (boost * (pivotDistanceExact / (pivotDistanceExact + distance)));
        }

        /**
         * Inverting the score computation is very hard due to all potential
         * rounding errors, so we binary search the maximum distance.
         */
        private long computeMaxDistance(float minScore, long previousMaxDistance) {
            assert score(0) >= minScore;
            if (score(previousMaxDistance) >= minScore) {
                // minScore did not decrease enough to require an update to the max distance
                return previousMaxDistance;
            }
            assert score(previousMaxDistance) < minScore;
            long min = 0, max = previousMaxDistance;
            // invariant: score(min) >= minScore && score(max) < minScore
            while (max - min > 1) {
                long mid = (min + max) >>> 1;
                float score = score(mid);
                if (score >= minScore) {
                    min = mid;
                } else {
                    max = mid;
                }
            }
            assert score(min) >= minScore;
            assert min == Long.MAX_VALUE || score(min + 1) < minScore;
            return min;
        }

        @Override
        public float score() throws IOException {
            if (docValues.advanceExact(docID()) == false) {
                return 0;
            }
            long v = docValues.longValue();
            // note: distance is unsigned
            long distance = Math.max(v, originExact) - Math.min(v, originExact);
            if (distance < 0) {
                // underflow
                // treat distances that are greater than MAX_VALUE as MAX_VALUE
                distance = Long.MAX_VALUE;
            }
            return score(distance);
        }

        @Override
        public DocIdSetIterator iterator() {
            // add indirection so that if 'it' is updated then it will
            // be taken into account
            return new DocIdSetIterator() {

                @Override
                public int nextDoc() throws IOException {
                    return doc = it.nextDoc();
                }

                @Override
                public int docID() {
                    return doc;
                }

                @Override
                public long cost() {
                    return it.cost();
                }

                @Override
                public int advance(int target) throws IOException {
                    return doc = it.advance(target);
                }
            };
        }

        @Override
        public float getMaxScore(int upTo) {
            return boost;
        }

        private int setMinCompetitiveScoreCounter = 0;

        @Override
        public void setMinCompetitiveScore(float minScore) throws IOException {
            if (minScore > boost) {
                it = DocIdSetIterator.empty();
                return;
            }

            // Start sampling if we get called too much
            setMinCompetitiveScoreCounter++;
            if (setMinCompetitiveScoreCounter > 256 && (setMinCompetitiveScoreCounter & 0x1f) != 0x1f) {
                return;
            }

            long previousMaxDistance = maxDistance;
            maxDistance = computeMaxDistance(minScore, maxDistance);
            if (maxDistance == previousMaxDistance) {
                // nothing to update
                return;
            }
            long minValue = originApprox - convertDistance(maxDistance) - 1;
            if (minValue > originApprox) {
                // underflow
                minValue = Long.MIN_VALUE;
            }
            long maxValue = originApprox + convertDistance(maxDistance) + 1;
            if (maxValue < originApprox) {
                // overflow
                maxValue = Long.MAX_VALUE;
            }

            final byte[] minValueAsBytes = new byte[Long.BYTES];
            LongPoint.encodeDimension(minValue, minValueAsBytes, 0);
            final byte[] maxValueAsBytes = new byte[Long.BYTES];
            LongPoint.encodeDimension(maxValue, maxValueAsBytes, 0);

            DocIdSetBuilder result = new DocIdSetBuilder(maxDoc);
            final int doc = docID();
            IntersectVisitor visitor = new IntersectVisitor() {

                DocIdSetBuilder.BulkAdder adder;

                @Override
                public void grow(int count) {
                    adder = result.grow(count);
                }

                @Override
                public void visit(int docID) {
                    if (docID <= doc) {
                        // Already visited or skipped
                        return;
                    }
                    adder.add(docID);
                }

                @Override
                public void visit(int docID, byte[] packedValue) {
                    if (docID <= doc) {
                        // Already visited or skipped
                        return;
                    }
                    if (Arrays.compareUnsigned(packedValue, 0, Long.BYTES, minValueAsBytes, 0, Long.BYTES) < 0) {
                        // Doc's value is too low, in this dimension
                        return;
                    }
                    if (Arrays.compareUnsigned(packedValue, 0, Long.BYTES, maxValueAsBytes, 0, Long.BYTES) > 0) {
                        // Doc's value is too high, in this dimension
                        return;
                    }

                    // Doc is in-bounds
                    adder.add(docID);
                }

                @Override
                public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                    if (Arrays.compareUnsigned(minPackedValue, 0, Long.BYTES, maxValueAsBytes, 0, Long.BYTES) > 0 ||
                        Arrays.compareUnsigned(maxPackedValue, 0, Long.BYTES, minValueAsBytes, 0, Long.BYTES) < 0) {
                        return Relation.CELL_OUTSIDE_QUERY;
                    }

                    if (Arrays.compareUnsigned(minPackedValue, 0, Long.BYTES, minValueAsBytes, 0, Long.BYTES) < 0 ||
                        Arrays.compareUnsigned(maxPackedValue, 0, Long.BYTES, maxValueAsBytes, 0, Long.BYTES) > 0) {
                        return Relation.CELL_CROSSES_QUERY;
                    }

                    return Relation.CELL_INSIDE_QUERY;
                }
            };

            final long currentQueryCost = Math.min(leadCost, it.cost());
            final long threshold = currentQueryCost >>> 3;
            long estimatedNumberOfMatches = pointValues.estimatePointCount(visitor); // runs in O(log(numPoints))
            // TODO: what is the right factor compared to the current disi? Is 8 optimal?
            if (estimatedNumberOfMatches >= threshold) {
                // the new range is not selective enough to be worth materializing
                return;
            }
            pointValues.intersect(visitor);
            it = result.build().iterator();
        }

    }
}
