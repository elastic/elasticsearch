/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

package org.apache.lucene.spatial.prefix.tree;

import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

import java.util.Collection;

/** The base for the original two SPT's: Geohash and Quad. Don't subclass this for new SPTs.
 * @lucene.internal
 *
 * NOTE: Will be removed upon commit of LUCENE-6422
 */
//public for RPT pruneLeafyBranches code
public abstract class LegacyCell implements Cell {

  // Important: A LegacyCell doesn't share state for getNextLevelCells(), and
  //  LegacySpatialPrefixTree assumes this in its simplify tree logic.

  private static final byte LEAF_BYTE = '+';//NOTE: must sort before letters & numbers

  //Arguably we could simply use a BytesRef, using an extra Object.
  protected byte[] bytes;//generally bigger to potentially hold a leaf
  protected int b_off;
  protected int b_len;//doesn't reflect leaf; same as getLevel()

  protected boolean isLeaf;

  /**
   * When set via getSubCells(filter), it is the relationship between this cell
   * and the given shape filter. Doesn't participate in shape equality.
   */
  protected SpatialRelation shapeRel;

  protected Shape shape;//cached

  /** Warning: Refers to the same bytes (no copy). If {@link #setLeaf()} is subsequently called then it
   * may modify bytes. */
  protected LegacyCell(byte[] bytes, int off, int len) {
    this.bytes = bytes;
    this.b_off = off;
    this.b_len = len;
    readLeafAdjust();
  }

  protected void readCell(BytesRef bytes) {
    shapeRel = null;
    shape = null;
    this.bytes = bytes.bytes;
    this.b_off = bytes.offset;
    this.b_len = (short) bytes.length;
    readLeafAdjust();
  }

  protected void readLeafAdjust() {
    isLeaf = (b_len > 0 && bytes[b_off + b_len - 1] == LEAF_BYTE);
    if (isLeaf)
      b_len--;
    if (getLevel() == getMaxLevels())
      isLeaf = true;
  }

  protected abstract SpatialPrefixTree getGrid();

  protected abstract int getMaxLevels();

  @Override
  public SpatialRelation getShapeRel() {
    return shapeRel;
  }

  @Override
  public void setShapeRel(SpatialRelation rel) {
    this.shapeRel = rel;
  }

  @Override
  public boolean isLeaf() {
    return isLeaf;
  }

  @Override
  public void setLeaf() {
    isLeaf = true;
  }

  @Override
  public BytesRef getTokenBytesWithLeaf(BytesRef result) {
    result = getTokenBytesNoLeaf(result);
    if (!isLeaf || getLevel() == getMaxLevels())
      return result;
    if (result.bytes.length < result.offset + result.length + 1) {
      assert false : "Not supposed to happen; performance bug";
      byte[] copy = new byte[result.length + 1];
      System.arraycopy(result.bytes, result.offset, copy, 0, result.length - 1);
      result.bytes = copy;
      result.offset = 0;
    }
    result.bytes[result.offset + result.length++] = LEAF_BYTE;
    return result;
  }

  @Override
  public BytesRef getTokenBytesNoLeaf(BytesRef result) {
    if (result == null)
      return new BytesRef(bytes, b_off, b_len);
    result.bytes = bytes;
    result.offset = b_off;
    result.length = b_len;
    return result;
  }

  @Override
  public int getLevel() {
    return b_len;
  }

  @Override
  public CellIterator getNextLevelCells(Shape shapeFilter) {
    assert getLevel() < getGrid().getMaxLevels();
    if (shapeFilter instanceof Point) {
      LegacyCell cell = getSubCell((Point) shapeFilter);
      cell.shapeRel = SpatialRelation.CONTAINS;
      return new SingletonCellIterator(cell);
    } else {
      return new FilterCellIterator(getSubCells().iterator(), shapeFilter);
    }
  }

  /**
   * Performant implementations are expected to implement this efficiently by
   * considering the current cell's boundary.
   * <p>
   * Precondition: Never called when getLevel() == maxLevel.
   * Precondition: this.getShape().relate(p) != DISJOINT.
   */
  protected abstract LegacyCell getSubCell(Point p);

  /**
   * Gets the cells at the next grid cell level that covers this cell.
   * Precondition: Never called when getLevel() == maxLevel.
   *
   * @return A set of cells (no dups), sorted, modifiable, not empty, not null.
   */
  protected abstract Collection<Cell> getSubCells();

  /**
   * {@link #getSubCells()}.size() -- usually a constant. Should be &gt;=2
   */
  public abstract int getSubCellsSize();

  @Override
  public boolean isPrefixOf(Cell c) {
    //Note: this only works when each level uses a whole number of bytes.
    LegacyCell cell = (LegacyCell)c;
    boolean result = sliceEquals(cell.bytes, cell.b_off, cell.b_len, bytes, b_off, b_len);
    assert result == StringHelper.startsWith(c.getTokenBytesNoLeaf(null), getTokenBytesNoLeaf(null));
    return result;
  }

  /** Copied from {@link org.apache.lucene.util.StringHelper#startsWith(org.apache.lucene.util.BytesRef, org.apache.lucene.util.BytesRef)}
   *  which calls this. This is to avoid creating a BytesRef.  */
  private static boolean sliceEquals(byte[] sliceToTest_bytes, int sliceToTest_offset, int sliceToTest_length,
                                     byte[] other_bytes, int other_offset, int other_length) {
    if (sliceToTest_length < other_length) {
      return false;
    }
    int i = sliceToTest_offset;
    int j = other_offset;
    final int k = other_offset + other_length;

    while (j < k) {
      if (sliceToTest_bytes[i++] != other_bytes[j++]) {
        return false;
      }
    }

    return true;
  }

  @Override
  public int compareToNoLeaf(Cell fromCell) {
    LegacyCell b = (LegacyCell) fromCell;
    return compare(bytes, b_off, b_len, b.bytes, b.b_off, b.b_len);
  }

  /** Copied from {@link org.apache.lucene.util.BytesRef#compareTo(org.apache.lucene.util.BytesRef)}.
   * This is to avoid creating a BytesRef. */
  protected static int compare(byte[] aBytes, int aUpto, int a_length, byte[] bBytes, int bUpto, int b_length) {
    final int aStop = aUpto + Math.min(a_length, b_length);
    while(aUpto < aStop) {
      int aByte = aBytes[aUpto++] & 0xff;
      int bByte = bBytes[bUpto++] & 0xff;

      int diff = aByte - bByte;
      if (diff != 0) {
        return diff;
      }
    }

    // One is a prefix of the other, or, they are equal:
    return a_length - b_length;
  }

  @Override
  public boolean equals(Object obj) {
    //this method isn't "normally" called; just in asserts/tests
    if (obj instanceof Cell) {
      Cell cell = (Cell) obj;
      return getTokenBytesWithLeaf(null).equals(cell.getTokenBytesWithLeaf(null));
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return getTokenBytesWithLeaf(null).hashCode();
  }

  @Override
  public String toString() {
    //this method isn't "normally" called; just in asserts/tests
    return getTokenBytesWithLeaf(null).utf8ToString();
  }

}
