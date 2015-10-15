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

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.document.GeoPointField;
import org.apache.lucene.index.FilteredTermsEnum;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.util.XGeoUtils;

/**
 * computes all ranges along a space-filling curve that represents
 * the given bounding box and enumerates all terms contained within those ranges
 *
 *  @lucene.experimental
 */
abstract class XGeoPointTermsEnum extends FilteredTermsEnum {
  protected final double minLon;
  protected final double minLat;
  protected final double maxLon;
  protected final double maxLat;

  protected Range currentRange;
  private final BytesRefBuilder currentCell = new BytesRefBuilder();
  private final BytesRefBuilder nextSubRange = new BytesRefBuilder();

  private final List<Range> rangeBounds = new LinkedList<>();

  // detail level should be a factor of PRECISION_STEP limiting the depth of recursion (and number of ranges)
  protected final short DETAIL_LEVEL;

  XGeoPointTermsEnum(final TermsEnum tenum, final double minLon, final double minLat,
                    final double maxLon, final double maxLat) {
    super(tenum);
    DETAIL_LEVEL = (short)(((XGeoUtils.BITS<<1)-computeMaxShift())/2);
    final long rectMinHash = XGeoUtils.mortonHash(minLon, minLat);
    final long rectMaxHash = XGeoUtils.mortonHash(maxLon, maxLat);
    this.minLon = XGeoUtils.mortonUnhashLon(rectMinHash);
    this.minLat = XGeoUtils.mortonUnhashLat(rectMinHash);
    this.maxLon = XGeoUtils.mortonUnhashLon(rectMaxHash);
    this.maxLat = XGeoUtils.mortonUnhashLat(rectMaxHash);

    computeRange(0L, (short) (((XGeoUtils.BITS) << 1) - 1));
    Collections.sort(rangeBounds);
  }

  /**
   * entry point for recursively computing ranges
   */
  private final void computeRange(long term, final short shift) {
    final long split = term | (0x1L<<shift);
    assert shift < 64;
    final long upperMax;
    if (shift < 63) {
      upperMax = term | ((1L << (shift+1))-1);
    } else {
      upperMax = 0xffffffffffffffffL;
    }
    final long lowerMax = split-1;

    relateAndRecurse(term, lowerMax, shift);
    relateAndRecurse(split, upperMax, shift);
  }

  /**
   * recurse to higher level precision cells to find ranges along the space-filling curve that fall within the
   * query box
   *
   * @param start starting value on the space-filling curve for a cell at a given res
   * @param end ending value on the space-filling curve for a cell at a given res
   * @param res spatial res represented as a bit shift (MSB is lower res)
   */
  private void relateAndRecurse(final long start, final long end, final short res) {
    final double minLon = XGeoUtils.mortonUnhashLon(start);
    final double minLat = XGeoUtils.mortonUnhashLat(start);
    final double maxLon = XGeoUtils.mortonUnhashLon(end);
    final double maxLat = XGeoUtils.mortonUnhashLat(end);

    final short level = (short)((XGeoUtils.BITS<<1)-res>>>1);

    // if cell is within and a factor of the precision step, or it crosses the edge of the shape add the range
    final boolean within = res % GeoPointField.PRECISION_STEP == 0 && cellWithin(minLon, minLat, maxLon, maxLat);
    if (within || (level == DETAIL_LEVEL && cellIntersectsShape(minLon, minLat, maxLon, maxLat))) {
      final short nextRes = (short)(res-1);
      if (nextRes % GeoPointField.PRECISION_STEP == 0) {
        rangeBounds.add(new Range(start, nextRes, !within));
        rangeBounds.add(new Range(start|(1L<<nextRes), nextRes, !within));
      } else {
        rangeBounds.add(new Range(start, res, !within));
      }
    } else if (level < DETAIL_LEVEL && cellIntersectsMBR(minLon, minLat, maxLon, maxLat)) {
      computeRange(start, (short) (res - 1));
    }
  }

  protected short computeMaxShift() {
    // in this case a factor of 4 brings the detail level to ~0.002/0.001 degrees lon/lat respectively (or ~222m/111m)
    return GeoPointField.PRECISION_STEP * 4;
  }

  /**
   * Determine whether the quad-cell crosses the shape
   */
  protected abstract boolean cellCrosses(final double minLon, final double minLat, final double maxLon, final double maxLat);

  /**
   * Determine whether quad-cell is within the shape
   */
  protected abstract boolean cellWithin(final double minLon, final double minLat, final double maxLon, final double maxLat);

  /**
   * Default shape is a rectangle, so this returns the same as {@code cellIntersectsMBR}
   */
  protected abstract boolean cellIntersectsShape(final double minLon, final double minLat, final double maxLon, final double maxLat);

  /**
   * Primary driver for cells intersecting shape boundaries
   */
  protected boolean cellIntersectsMBR(final double minLon, final double minLat, final double maxLon, final double maxLat) {
    return XGeoUtils.rectIntersects(minLon, minLat, maxLon, maxLat, this.minLon, this.minLat, this.maxLon, this.maxLat);
  }

  /**
   * Return whether quad-cell contains the bounding box of this shape
   */
  protected boolean cellContains(final double minLon, final double minLat, final double maxLon, final double maxLat) {
    return XGeoUtils.rectWithin(this.minLon, this.minLat, this.maxLon, this.maxLat, minLon, minLat, maxLon, maxLat);
  }

  public boolean boundaryTerm() {
    if (currentRange == null) {
      throw new IllegalStateException("GeoPointTermsEnum empty or not initialized");
    }
    return currentRange.boundary;
  }

  private void nextRange() {
    currentRange = rangeBounds.remove(0);
    currentRange.fillBytesRef(currentCell);
  }

  @Override
  protected final BytesRef nextSeekTerm(BytesRef term) {
    while (!rangeBounds.isEmpty()) {
      if (currentRange == null) {
        nextRange();
      }

      // if the new upper bound is before the term parameter, the sub-range is never a hit
      if (term != null && term.compareTo(currentCell.get()) > 0) {
        nextRange();
        if (!rangeBounds.isEmpty()) {
          continue;
        }
      }
      // never seek backwards, so use current term if lower bound is smaller
      return (term != null && term.compareTo(currentCell.get()) > 0) ?
              term : currentCell.get();
    }

    // no more sub-range enums available
    assert rangeBounds.isEmpty();
    return null;
  }

  /**
   * The two-phase query approach. {@link #nextSeekTerm} is called to obtain the next term that matches a numeric
   * range of the bounding box. Those terms that pass the initial range filter are then compared against the
   * decoded min/max latitude and longitude values of the bounding box only if the range is not a "boundary" range
   * (e.g., a range that straddles the boundary of the bbox).
   * @param term term for candidate document
   * @return match status
   */
  @Override
  protected AcceptStatus accept(BytesRef term) {
    // validate value is in range
    while (currentCell == null || term.compareTo(currentCell.get()) > 0) {
      if (rangeBounds.isEmpty()) {
        return AcceptStatus.END;
      }
      // peek next sub-range, only seek if the current term is smaller than next lower bound
      rangeBounds.get(0).fillBytesRef(this.nextSubRange);
      if (term.compareTo(this.nextSubRange.get()) < 0) {
        return AcceptStatus.NO_AND_SEEK;
      }
      // step forward to next range without seeking, as next range is less or equal current term
      nextRange();
    }

    return AcceptStatus.YES;
  }

  protected abstract boolean postFilter(final double lon, final double lat);

  /**
   * Internal class to represent a range along the space filling curve
   */
  protected final class Range implements Comparable<Range> {
    final short shift;
    final long start;
    final boolean boundary;

    Range(final long lower, final short shift, boolean boundary) {
      this.boundary = boundary;
      this.start = lower;
      this.shift = shift;
    }

    /**
     * Encode as a BytesRef using a reusable object. This allows us to lazily create the BytesRef (which is
     * quite expensive), only when we need it.
     */
    private void fillBytesRef(BytesRefBuilder result) {
      assert result != null;
      NumericUtils.longToPrefixCoded(start, shift, result);
    }

    @Override
    public int compareTo(Range other) {
      final int result = Short.compare(this.shift, other.shift);
      if (result == 0) {
        return Long.compare(this.start, other.start);
      }
      return result;
    }
  }
}
