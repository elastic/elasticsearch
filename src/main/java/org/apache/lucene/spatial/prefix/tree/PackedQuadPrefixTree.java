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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;
import com.spatial4j.core.shape.impl.RectangleImpl;
import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Subclassing QuadPrefixTree this {@link SpatialPrefixTree} uses the compact QuadCell encoding described in
 * {@link PackedQuadCell}
 *
 * @lucene.experimental
 *
 * NOTE: Will be removed upon commit of LUCENE-6422
 */
public class PackedQuadPrefixTree extends QuadPrefixTree {
  public static final byte[] QUAD = new byte[] {0x00, 0x01, 0x02, 0x03};
  public static final int MAX_LEVELS_POSSIBLE = 29;

  private boolean leafyPrune = true;

  public static class Factory extends QuadPrefixTree.Factory {
    @Override
    protected SpatialPrefixTree newSPT() {
      if (maxLevels > MAX_LEVELS_POSSIBLE) {
        throw new IllegalArgumentException("maxLevels " + maxLevels + " exceeds maximum value " + MAX_LEVELS_POSSIBLE);
      }
      return new PackedQuadPrefixTree(ctx, maxLevels);
    }
  }

  public PackedQuadPrefixTree(SpatialContext ctx, int maxLevels) {
    super(ctx, maxLevels);
  }

  @Override
  public Cell getWorldCell() {
    return new PackedQuadCell(0x0L);
  }
  @Override
  public Cell getCell(Point p, int level) {
    List<Cell> cells = new ArrayList<>(1);
    build(xmid, ymid, 0, cells, 0x0L, ctx.makePoint(p.getX(),p.getY()), level);
    return cells.get(0);//note cells could be longer if p on edge
  }

  protected void build(double x, double y, int level, List<Cell> matches, long term, Shape shape, int maxLevel) {
    double w = levelW[level] / 2;
    double h = levelH[level] / 2;

    // Z-Order
    // http://en.wikipedia.org/wiki/Z-order_%28curve%29
    checkBattenberg(QUAD[0], x - w, y + h, level, matches, term, shape, maxLevel);
    checkBattenberg(QUAD[1], x + w, y + h, level, matches, term, shape, maxLevel);
    checkBattenberg(QUAD[2], x - w, y - h, level, matches, term, shape, maxLevel);
    checkBattenberg(QUAD[3], x + w, y - h, level, matches, term, shape, maxLevel);
  }

  protected void checkBattenberg(byte quad, double cx, double cy, int level, List<Cell> matches,
                               long term, Shape shape, int maxLevel) {
    // short-circuit if we find a match for the point (no need to continue recursion)
    if (shape instanceof Point && !matches.isEmpty())
      return;
    double w = levelW[level] / 2;
    double h = levelH[level] / 2;

    SpatialRelation v = shape.relate(ctx.makeRectangle(cx - w, cx + w, cy - h, cy + h));

    if (SpatialRelation.DISJOINT == v) {
      return;
    }

    // set bits for next level
    term |= (((long)(quad))<<(64-(++level<<1)));
    // increment level
    term = ((term>>>1)+1)<<1;

    if (SpatialRelation.CONTAINS == v || (level >= maxLevel)) {
      matches.add(new PackedQuadCell(term, v.transpose()));
    } else {// SpatialRelation.WITHIN, SpatialRelation.INTERSECTS
      build(cx, cy, level, matches, term, shape, maxLevel);
    }
  }

  @Override
  public Cell readCell(BytesRef term, Cell scratch) {
    PackedQuadCell cell = (PackedQuadCell) scratch;
    if (cell == null)
      cell = (PackedQuadCell) getWorldCell();
    cell.readCell(term);
    return cell;
  }

  @Override
  public CellIterator getTreeCellIterator(Shape shape, int detailLevel) {
    return new PrefixTreeIterator(shape);
  }

  public void setPruneLeafyBranches( boolean pruneLeafyBranches ) {
    this.leafyPrune = pruneLeafyBranches;
  }

  /**
   * PackedQuadCell Binary Representation is as follows
   * CCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCCDDDDDL
   *
   * Where C = Cell bits (2 per quad)
   *       D = Depth bits (5 with max of 29 levels)
   *       L = isLeaf bit
   */
  public class PackedQuadCell extends QuadCell {
    private long term;

    PackedQuadCell(long term) {
      super(null, 0, 0);
      this.term = term;
      this.b_off = 0;
      this.bytes = longToByteArray(this.term);
      this.b_len = 8;
      readLeafAdjust();
    }

    PackedQuadCell(long term, SpatialRelation shapeRel) {
      this(term);
      this.shapeRel = shapeRel;
    }

    @Override
    protected void readCell(BytesRef bytes) {
      shapeRel = null;
      shape = null;
      this.bytes = bytes.bytes;
      this.b_off = bytes.offset;
      this.b_len = (short) bytes.length;
      this.term = longFromByteArray(this.bytes, bytes.offset);
      readLeafAdjust();
    }

    private final int getShiftForLevel(final int level) {
      return 64 - (level<<1);
    }

    public boolean isEnd(final int level, final int shift) {
      return (term != 0x0L && ((((0x1L<<(level<<1))-1)-(term>>>shift)) == 0x0L));
    }

    /**
     * Get the next cell in the tree without using recursion. descend parameter requests traversal to the child nodes,
     * setting this to false will step to the next sibling.
     * Note: This complies with lexicographical ordering, once you've moved to the next sibling there is no backtracking.
     */
    public PackedQuadCell nextCell(boolean descend) {
      final int level = getLevel();
      final int shift = getShiftForLevel(level);
      // base case: can't go further
      if ( (!descend && isEnd(level, shift)) || isEnd(maxLevels, getShiftForLevel(maxLevels))) {
        return null;
      }
      long newTerm;
      final boolean isLeaf = (term&0x1L)==0x1L;
      // if descend requested && we're not at the maxLevel
      if ((descend && !isLeaf && (level != maxLevels)) || level == 0) {
        // simple case: increment level bits (next level)
        newTerm = ((term>>>1)+0x1L)<<1;
      } else {  // we're not descending or we can't descend
        newTerm = term + (0x1L<<shift);
        // we're at the last sibling...force descend
        if (((term>>>shift)&0x3L) == 0x3L) {
          // adjust level for number popping up
          newTerm = ((newTerm>>>1) - (Long.numberOfTrailingZeros(newTerm>>>shift)>>>1))<<1;
        }
      }
      return new PackedQuadCell(newTerm);
    }

    @Override
    protected void readLeafAdjust() {
      isLeaf = ((0x1L)&term) == 0x1L;
      if (getLevel() == getMaxLevels()) {
        isLeaf = true;
      }
    }

    @Override
    public BytesRef getTokenBytesWithLeaf(BytesRef result) {
      if (isLeaf) {
        term |= 0x1L;
      }
      return getTokenBytesNoLeaf(result);
    }

    @Override
    public BytesRef getTokenBytesNoLeaf(BytesRef result) {
      if (result == null)
        return new BytesRef(bytes, b_off, b_len);
      result.bytes = longToByteArray(this.term);
      result.offset = 0;
      result.length = result.bytes.length;
      return result;
    }

    @Override
    public int compareToNoLeaf(Cell fromCell) {
      PackedQuadCell b = (PackedQuadCell) fromCell;
      final long thisTerm = (((0x1L)&term) == 0x1L) ? term-1 : term;
      final long fromTerm = (((0x1L)&b.term) == 0x1L) ? b.term-1 : b.term;
      final int result = compare(longToByteArray(thisTerm), 0, 8, longToByteArray(fromTerm), 0, 8);
      return result;
    }

    @Override
    public int getLevel() {
      int l = (int)((term >>> 1)&0x1FL);
      return l;
    }

    @Override
    protected Collection<Cell> getSubCells() {
      List<Cell> cells = new ArrayList<>(4);
      PackedQuadCell pqc = (PackedQuadCell)(new PackedQuadCell(((term&0x1)==0x1) ? this.term-1 : this.term))
          .nextCell(true);
      cells.add(pqc);
      cells.add((pqc = (PackedQuadCell) (pqc.nextCell(false))));
      cells.add((pqc = (PackedQuadCell) (pqc.nextCell(false))));
      cells.add(pqc.nextCell(false));
      return cells;
    }

    @Override
    protected QuadCell getSubCell(Point p) {
      return (PackedQuadCell) PackedQuadPrefixTree.this.getCell(p, getLevel() + 1);//not performant!
    }

    @Override
    public boolean isPrefixOf(Cell c) {
      PackedQuadCell cell = (PackedQuadCell)c;
      return (this.term==0x0L) ? true : isInternalPrefix(cell);
    }

    protected boolean isInternalPrefix(PackedQuadCell c) {
      final int shift = 64 - (getLevel()<<1);
      return ((term>>>shift)-(c.term>>>shift)) == 0x0L;
    }

    protected long concat(byte postfix) {
      // extra leaf bit
      return this.term | (((long)(postfix))<<((getMaxLevels()-getLevel()<<1)+6));
    }

    /**
     * Constructs a bounding box shape out of the encoded cell
     */
    @Override
    protected Rectangle makeShape() {
      double xmin = PackedQuadPrefixTree.this.xmin;
      double ymin = PackedQuadPrefixTree.this.ymin;
      int level = getLevel();

      byte b;
      for (short l=0, i=1; l<level; ++l, ++i) {
        b = (byte) ((term>>>(64-(i<<1))) & 0x3L);

        switch (b) {
          case 0x00:
            ymin += levelH[l];
            break;
          case 0x01:
            xmin += levelW[l];
            ymin += levelH[l];
            break;
          case 0x02:
            break;//nothing really
          case 0x03:
            xmin += levelW[l];
            break;
          default:
            throw new RuntimeException("unexpected quadrant");
        }
      }

      double width, height;
      if (level > 0) {
        width = levelW[level - 1];
        height = levelH[level - 1];
      } else {
        width = gridW;
        height = gridH;
      }
      return new RectangleImpl(xmin, xmin + width, ymin, ymin + height, ctx);
    }

    private long fromBytes(byte b1, byte b2, byte b3, byte b4, byte b5, byte b6, byte b7, byte b8) {
      return ((long)b1 & 255L) << 56 | ((long)b2 & 255L) << 48 | ((long)b3 & 255L) << 40
          | ((long)b4 & 255L) << 32 | ((long)b5 & 255L) << 24 | ((long)b6 & 255L) << 16
          | ((long)b7 & 255L) << 8 | (long)b8 & 255L;
    }

    private byte[] longToByteArray(long value) {
      byte[] result = new byte[8];
      for(int i = 7; i >= 0; --i) {
        result[i] = (byte)((int)(value & 255L));
        value >>= 8;
      }
      return result;
    }

    private long longFromByteArray(byte[] bytes, int ofs) {
      assert bytes.length >= 8;
      return fromBytes(bytes[0+ofs], bytes[1+ofs], bytes[2+ofs], bytes[3+ofs],
          bytes[4+ofs], bytes[5+ofs], bytes[6+ofs], bytes[7+ofs]);
    }

    /**
     * Used for debugging, this will print the bits of the cell
     */
    @Override
    public String toString() {
      String s = "";
      for(int i = 0; i < Long.numberOfLeadingZeros(term); i++) {
        s+='0';
      }
      if (term != 0)
        s += Long.toBinaryString(term);
      return s;
    }
  } // PackedQuadCell

    protected class PrefixTreeIterator extends CellIterator {
    private Shape shape;
    private PackedQuadCell thisCell;
    private PackedQuadCell nextCell;

    private short leaves;
    private short level;
    private final short maxLevels;
    private CellIterator pruneIter;

    PrefixTreeIterator(Shape shape) {
      this.shape = shape;
      this.thisCell = ((PackedQuadCell)(getWorldCell())).nextCell(true);
      this.maxLevels = (short)thisCell.getMaxLevels();
      this.nextCell = null;
    }

    @Override
    public boolean hasNext() {
      if (nextCell != null) {
        return true;
      }
      SpatialRelation rel;
      // loop until we're at the end of the quad tree or we hit a relation
      while (thisCell != null) {
        rel = thisCell.getShape().relate(shape);
        if (rel == SpatialRelation.DISJOINT) {
          thisCell = thisCell.nextCell(false);
        } else { // within || intersects || contains
          thisCell.setShapeRel(rel);
          nextCell = thisCell;
          if (rel == SpatialRelation.WITHIN) {
            thisCell.setLeaf();
            thisCell = thisCell.nextCell(false);
          } else {  // intersects || contains
            level = (short) (thisCell.getLevel());
            if (level == maxLevels || pruned(rel)) {
              thisCell.setLeaf();
              if (shape instanceof Point) {
                thisCell.setShapeRel(SpatialRelation.WITHIN);
                thisCell = null;
              } else {
                thisCell = thisCell.nextCell(false);
              }
              break;
            }
            thisCell = thisCell.nextCell(true);
          }
          break;
        }
      }
      return nextCell != null;
    }

    private boolean pruned(SpatialRelation rel) {
      if (rel == SpatialRelation.INTERSECTS && leafyPrune && level == maxLevels-1) {
        for (leaves=0, pruneIter=thisCell.getNextLevelCells(shape); pruneIter.hasNext(); pruneIter.next(), ++leaves);
        return leaves == 4;
      }
      return false;
    }

    @Override
    public Cell next() {
      if (nextCell == null) {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }
      }
      // overriding since this implementation sets thisCell in hasNext
      Cell temp = nextCell;
      nextCell = null;
      return temp;
    }

    @Override
    public void remove() {
      //no-op
    }
  }
}
