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

import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.ConstantScoreWeight;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BitSetIterator;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Abstract class for range queries against single or multidimensional points such as
 * {@link IntPoint}.
 * <p>
 * This is for subclasses and works on the underlying binary encoding: to
 * create range queries for lucene's standard {@code Point} types, refer to factory
 * methods on those classes, e.g. {@link IntPoint#newRangeQuery IntPoint.newRangeQuery()} for
 * fields indexed with {@link IntPoint}.
 * <p>
 * For a single-dimensional field this query is a simple range query; in a multi-dimensional field it's a box shape.
 * @see PointValues
 */
public class TwoPhaseLongRangeQuery extends Query {
  final String field;
  final long lowerExact;
  final long upperExact;
  final long lowerApprox;
  final long upperApprox;

  /**
   * Expert: create a multidimensional range query for point values.
   *
   * @param field field name. must not be {@code null}.
   * @param lowerExact lower portion of the range (inclusive).
   * @param upperExact upper portion of the range (inclusive).
   * @throws IllegalArgumentException if {@code field} is null, or if {@code lowerValue.length != upperValue.length}
   */
  public TwoPhaseLongRangeQuery(String field, long lowerExact, long upperExact, long lowerApprox, long upperApprox) {
      this.field = field;
      this.lowerExact = lowerExact;
      this.upperExact = upperExact;
      this.lowerApprox = lowerApprox;
      this.upperApprox = upperApprox;
  }

  @Override
  public void visit(QueryVisitor visitor) {
    if (visitor.acceptField(field)) {
      visitor.visitLeaf(this);
    }
  }

  @Override
  public final Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
      byte[] lowerInc = new byte[Long.BYTES];
      NumericUtils.longToSortableBytes(lowerApprox + 1, lowerInc, 0);
      byte[] lowerExc = new byte[Long.BYTES];
      NumericUtils.longToSortableBytes(lowerApprox, lowerExc, 0);
      byte[] upperInc = new byte[Long.BYTES];
      NumericUtils.longToSortableBytes(upperApprox - 1, upperInc, 0);
      byte[] upperExc = new byte[Long.BYTES];
      NumericUtils.longToSortableBytes(upperApprox, upperExc, 0);

      final ApproximatedQuery phase1 = new ApproximatedQuery(lowerInc, lowerExc, upperInc, upperExc);

      return new ConstantScoreWeight(this, boost) {


          @Override
          public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
              LeafReader reader = context.reader();

              PointValues values = reader.getPointValues(field);
              if (values == null) {
                  // No docs in this segment/field indexed any points
                  return null;
              }

              if (values.getNumIndexDimensions() != 1) {
                  throw new IllegalArgumentException("field=\"" + field + "\" was indexed with numIndexDimensions=" + values.getNumIndexDimensions() + " but this query has numDims=" + 1);
              }
              if (Long.BYTES != values.getBytesPerDimension()) {
                  throw new IllegalArgumentException("field=\"" + field + "\" was indexed with bytesPerDim=" + values.getBytesPerDimension() + " but this query has bytesPerDim=" + Long.BYTES);
              }
              final Weight weight = this;
              if (values.getDocCount() == reader.maxDoc()) {
                  final byte[] fieldPackedLower = values.getMinPackedValue();
                  final byte[] fieldPackedUpper = values.getMaxPackedValue();
                  if (Arrays.compareUnsigned(lowerExc, 0, Long.BYTES, fieldPackedLower, 0, Long.BYTES) < 0
                      && Arrays.compareUnsigned(upperExc, 0, Long.BYTES, fieldPackedUpper, 0, Long.BYTES) > 0) {
                      return new ScorerSupplier() {
                          @Override
                          public Scorer get(long leadCost) {
                              return new ConstantScoreScorer(weight, score(), scoreMode, DocIdSetIterator.all(reader.maxDoc()));
                          }

                          @Override
                          public long cost() {
                              return reader.maxDoc();
                          }
                      };
                  }
              }
              final DocIdSetIterator exactIterator;
              final DocIdSetIterator approxDISI;
              if (values.getDocCount() == reader.maxDoc()
                  && values.getDocCount() == values.size()
                  && phase1.cost(values) > reader.maxDoc() / 2) {
                  // If all docs have exactly one value and the cost is greater
                  // than half the leaf size then maybe we can make things faster
                  // by computing the set of documents that do NOT match the range
                  final FixedBitSet result = new FixedBitSet(reader.maxDoc());
                  final FixedBitSet approx = new FixedBitSet(reader.maxDoc());
                  result.set(0, reader.maxDoc());
                  approx.set(0, reader.maxDoc());
                  int[] cost = new int[] { reader.maxDoc() };
                  phase1.execute(values, result, approx, cost);
                  exactIterator = new BitSetIterator(result, cost[0]);
                  approxDISI = phase1.hasApproximated ? new BitSetIterator(approx, cost[0]) : null;
              } else {
                  DocIdSetBuilder result = new DocIdSetBuilder(reader.maxDoc(), values, field);
                  DocIdSetBuilder approximation = new DocIdSetBuilder(reader.maxDoc(), values, field);
                  phase1.execute(values, result, approximation);
                  exactIterator = result.build().iterator();
                  approxDISI = phase1.hasApproximated ? approximation.build().iterator() : null;
              }

              if (approxDISI == null) {
                  return new ScorerSupplier() {

                      @Override
                      public Scorer get(long leadCost) {
                          return new ConstantScoreScorer(weight, score(), scoreMode, exactIterator);
                      }

                      @Override
                      public long cost() {
                          return exactIterator.cost();
                      }
                  };
              }

              final SortedNumericDocValues docValues = reader.getSortedNumericDocValues(field);
              final TwoPhaseIterator twoPhaseIterator = new TwoPhaseIterator(approxDISI) {

                  @Override
                  public boolean matches() throws IOException {
                      final int doc = approxDISI.docID();
                      if (exactIterator != null) {
                          if (exactIterator.docID() < doc) {
                              exactIterator.advance(doc);
                          }
                          if (exactIterator.docID() == doc) {
                              return true;
                          }
                      }
                      return matches(doc);
                  }

                  private boolean matches(int docId) throws IOException {
                      if (docValues.advanceExact(docId)) {
                          int count = docValues.docValueCount();
                          for (int i = 0; i < count; i++) {
                              final long value = docValues.nextValue();
                              if (value <= upperExact && value >= lowerExact) {
                                  return true;
                              }
                          }
                      }
                      return false;
                  }

                  @Override
                  public float matchCost() {
                      return 100; // TODO: use cost of exactIterator.advance() and predFuncValues.cost()
                  }
              };
              return new ScorerSupplier() {
                  @Override
                  public Scorer get(long leadCost) {
                      return new ConstantScoreScorer(weight, score(), scoreMode, twoPhaseIterator);
                  }

                  @Override
                  public long cost() {
                      return exactIterator.cost();
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

          @Override
          public boolean isCacheable(LeafReaderContext ctx) {
              return true;
          }

      };
  }

  public String getField() {
    return field;
  }

  @Override
  public final int hashCode() {
      int hash = classHash();
      hash = 31 * hash + field.hashCode();
      hash = 31 * hash + Long.hashCode(upperExact);
      hash = 31 * hash + Long.hashCode(lowerExact);
      hash = 31 * hash + Long.hashCode(upperApprox);
      hash = 31 * hash + Long.hashCode(lowerApprox);
      return hash;
  }

  @Override
  public final boolean equals(Object o) {
    return sameClassAs(o) &&
           equalsTo(getClass().cast(o));
  }

  private boolean equalsTo(TwoPhaseLongRangeQuery other) {
    return Objects.equals(field, other.field) &&
        upperExact == other.upperExact && lowerExact == other.lowerExact &&
        upperApprox == other.upperApprox && lowerApprox == other.lowerApprox;
  }

  @Override
  public final String toString(String field) {
    final StringBuilder sb = new StringBuilder();
    if (this.field.equals(field) == false) {
      sb.append(this.field);
      sb.append(':');
    }
    sb.append('[');
    sb.append(lowerExact);
    sb.append(" TO ");
    sb.append(upperExact);
    sb.append(']');
    return sb.toString();
  }

  private static class ApproximatedQuery {
      private final byte[] lowerPointInc;
      private final byte[] lowerPointExc;
      private final byte[] upperPointInc;
      private final byte[] upperPointExc;
      private boolean hasApproximated;
      private long cost;

      ApproximatedQuery(byte[] lowerPointInc, byte[] lowerPointExc, byte[] upperPointInc, byte[] upperPointExc) {
          this.lowerPointInc = lowerPointInc;
          this.lowerPointExc = lowerPointExc;
          this.upperPointInc = upperPointInc;
          this.upperPointExc = upperPointExc;
          cost = -1;
      }

      void execute(PointValues values, DocIdSetBuilder result, DocIdSetBuilder approximation) throws IOException {
          final IntersectVisitor visitor = getIntersectVisitor(result, approximation);
          values.intersect(visitor);
      }

      void execute(PointValues values, FixedBitSet result, FixedBitSet approximation, int[] cost) throws IOException {
          final IntersectVisitor visitor = getInverseIntersectVisitor(result, approximation, cost);
          values.intersect(visitor);
      }

      long cost(PointValues values) {
          if (cost == -1) {
              // Computing the cost may be expensive, so only do it if necessary
              cost = values.estimateDocCount(getIntersectVisitor(null, null));
              assert cost >= 0;
          }
          return cost;
      }

      private IntersectVisitor getIntersectVisitor(DocIdSetBuilder result, DocIdSetBuilder approx) {
          return new IntersectVisitor() {

              DocIdSetBuilder.BulkAdder adderExact;
              DocIdSetBuilder.BulkAdder adderApprox;

              @Override
              public void grow(int count) {
                  adderExact = result.grow(count);
                  adderApprox = approx.grow(count);
              }

              @Override
              public void visit(int docID) {
                  adderExact.add(docID);
                  adderApprox.add(docID);
              }

              @Override
              public void visit(int docID, byte[] packedValue) {
                  if (matches(packedValue)) {
                      adderApprox.add(docID);
                      if (addToExact(packedValue)) {
                          adderExact.add(docID);
                      } else {
                          hasApproximated = true;
                      }
                  }
              }

              @Override
              public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                  if (matches(packedValue)) {
                      boolean addExact = addToExact(packedValue);
                      int docID;
                      while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                          adderApprox.add(docID);
                          if (addExact) {
                              adderExact.add(docID);
                          } else {
                              hasApproximated = true;
                          }
                      }
                  }
              }

              @Override
              public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                  return relate(minPackedValue, maxPackedValue);
              }
          };
      }

      private IntersectVisitor getInverseIntersectVisitor(FixedBitSet exact, FixedBitSet approx, int[] cost) {
          return new IntersectVisitor() {

              @Override
              public void visit(int docID) {
                  exact.clear(docID);
                  approx.clear(docID);
                  cost[0]--;
              }

              @Override
              public void visit(int docID, byte[] packedValue) {
                  if (addToExact(packedValue) == false) {
                      exact.clear(docID);
                      if (matches(packedValue) == false) {
                          approx.clear(docID);
                      } else {
                          hasApproximated = true;
                      }
                  }
              }

              @Override
              public void visit(DocIdSetIterator iterator, byte[] packedValue) throws IOException {
                  if (addToExact(packedValue) == false) {
                      boolean matches = matches(packedValue);
                      int docID;
                      while ((docID = iterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                          cost[0]--;
                          exact.clear(docID);
                          if (matches == false) {
                              approx.clear(docID);
                          } else {
                              hasApproximated = true;
                          }
                      }
                  }
              }

              @Override
              public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                  Relation relation = relate(minPackedValue, maxPackedValue);
                  switch (relation) {
                      case CELL_INSIDE_QUERY:
                          // all points match, skip this subtree
                          return Relation.CELL_OUTSIDE_QUERY;
                      case CELL_OUTSIDE_QUERY:
                          // none of the points match, clear all documents
                          return Relation.CELL_INSIDE_QUERY;
                      default:
                          return relation;
                  }
              }
          };
      }

      private boolean matches(byte[] packedValue) {
          if (Arrays.compareUnsigned(packedValue, 0, Long.BYTES, lowerPointExc, 0, Long.BYTES) < 0) {
              // Doc's value is too low
              return false;
          }
          if (Arrays.compareUnsigned(packedValue, 0, Long.BYTES, upperPointExc, 0, Long.BYTES) > 0) {
              // Doc's value is too high, in this dimension
              return false;
          }
          return true;
      }

      private boolean addToExact(byte[] packedValue) {
          if (Arrays.compareUnsigned(packedValue, 0, Long.BYTES, lowerPointInc, 0, Long.BYTES) < 0) {
              // Doc's value is too low
              return false;
          }
          if (Arrays.compareUnsigned(packedValue, 0, Long.BYTES, upperPointInc, 0, Long.BYTES) > 0) {
              // Doc's value is too high, in this dimension
              return false;
          }
          return true;
      }

      private Relation relate(byte[] minPackedValue, byte[] maxPackedValue) {
          if (Arrays.compareUnsigned(minPackedValue, 0, Long.BYTES, upperPointExc, 0, Long.BYTES) > 0 ||
              Arrays.compareUnsigned(maxPackedValue, 0, Long.BYTES, lowerPointExc, 0, Long.BYTES) < 0) {
              return Relation.CELL_OUTSIDE_QUERY;
          } else if (Arrays.compareUnsigned(minPackedValue, 0, Long.BYTES, lowerPointInc, 0, Long.BYTES) < 0 ||
              Arrays.compareUnsigned(maxPackedValue, 0, Long.BYTES, upperPointInc, 0, Long.BYTES) > 0) {
              return Relation.CELL_CROSSES_QUERY;
          } else {
              return Relation.CELL_INSIDE_QUERY;
          }
      }
  }
}
