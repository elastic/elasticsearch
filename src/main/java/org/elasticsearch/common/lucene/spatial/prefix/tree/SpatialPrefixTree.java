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

package org.elasticsearch.common.lucene.spatial.prefix.tree;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Shape;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * A spatial Prefix Tree, or Trie, which decomposes shapes into prefixed strings at variable lengths corresponding to
 * variable precision.  Each string corresponds to a spatial region.
 * <p/>
 * Implementations of this class should be thread-safe and immutable once initialized.
 *
 * @lucene.experimental
 */
public abstract class SpatialPrefixTree {

  protected static final Charset UTF8 = Charset.forName("UTF-8");

  protected final int maxLevels;

  protected final SpatialContext ctx;

  public SpatialPrefixTree(SpatialContext ctx, int maxLevels) {
    assert maxLevels > 0;
    this.ctx = ctx;
    this.maxLevels = maxLevels;
  }

  public SpatialContext getSpatialContext() {
    return ctx;
  }

  public int getMaxLevels() {
    return maxLevels;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(maxLevels:" + maxLevels + ",ctx:" + ctx + ")";
  }

  /**
   * Returns the level of the largest grid in which its longest side is less
   * than or equal to the provided distance (in degrees). Consequently {@code
   * dist} acts as an error epsilon declaring the amount of detail needed in the
   * grid, such that you can get a grid with just the right amount of
   * precision.
   *
   * @param dist >= 0
   * @return level [1 to maxLevels]
   */
  public abstract int getLevelForDistance(double dist);

  //TODO double getDistanceForLevel(int level)

  private transient Node worldNode;//cached

  /**
   * Returns the level 0 cell which encompasses all spatial data. Equivalent to {@link #getNode(String)} with "".
   * This cell is threadsafe, just like a spatial prefix grid is, although cells aren't
   * generally threadsafe.
   * TODO rename to getTopCell or is this fine?
   */
  public Node getWorldNode() {
    if (worldNode == null) {
      worldNode = getNode("");
    }
    return worldNode;
  }

  /**
   * The cell for the specified token. The empty string should be equal to {@link #getWorldNode()}.
   * Precondition: Never called when token length > maxLevel.
   */
  public abstract Node getNode(String token);

  public abstract Node getNode(byte[] bytes, int offset, int len);

  public final Node getNode(byte[] bytes, int offset, int len, Node target) {
    if (target == null) {
      return getNode(bytes, offset, len);
    }

    target.reset(bytes, offset, len);
    return target;
  }

  protected Node getNode(Point p, int level) {
    return getNodes(p, level, false).get(0);
  }

  /**
   * Gets the intersecting & including cells for the specified shape, without exceeding detail level.
   * The result is a set of cells (no dups), sorted. Unmodifiable.
   * <p/>
   * This implementation checks if shape is a Point and if so uses an implementation that
   * recursively calls {@link Node#getSubCell(com.spatial4j.core.shape.Point)}. Cell subclasses
   * ideally implement that method with a quick implementation, otherwise, subclasses should
   * override this method to invoke {@link #getNodesAltPoint(com.spatial4j.core.shape.Point, int, boolean)}.
   * TODO consider another approach returning an iterator -- won't build up all cells in memory.
   */
  public List<Node> getNodes(Shape shape, int detailLevel, boolean inclParents) {
    if (detailLevel > maxLevels) {
      throw new IllegalArgumentException("detailLevel > maxLevels");
    }

    List<Node> cells;
    if (shape instanceof Point) {
      //optimized point algorithm
      final int initialCapacity = inclParents ? 1 + detailLevel : 1;
      cells = new ArrayList<Node>(initialCapacity);
      recursiveGetNodes(getWorldNode(), (Point) shape, detailLevel, true, cells);
      assert cells.size() == initialCapacity;
    } else {
      cells = new ArrayList<Node>(inclParents ? 1024 : 512);
      recursiveGetNodes(getWorldNode(), shape, detailLevel, inclParents, cells);
    }
    if (inclParents) {
      Node c = cells.remove(0);//remove getWorldNode()
      assert c.getLevel() == 0;
    }
    return cells;
  }

  private void recursiveGetNodes(Node node, Shape shape, int detailLevel, boolean inclParents,
                                 Collection<Node> result) {
    if (node.isLeaf()) {//cell is within shape
      result.add(node);
      return;
    }
    final Collection<Node> subCells = node.getSubCells(shape);
    if (node.getLevel() == detailLevel - 1) {
      if (subCells.size() == node.getSubCellsSize() && !inclParents) {
        // A bottom level (i.e. detail level) optimization where all boxes intersect, so use parent cell.
        // Can optimize at only one of index time or query/filter time; the !inclParents
        // condition above means we do not optimize at index time.
        node.setLeaf();
        result.add(node);
      } else {
        if (inclParents)
          result.add(node);
        for (Node subCell : subCells) {
          subCell.setLeaf();
        }
        result.addAll(subCells);
      }
    } else {
      if (inclParents) {
        result.add(node);
      }
      for (Node subCell : subCells) {
        recursiveGetNodes(subCell, shape, detailLevel, inclParents, result);//tail call
      }
    }
  }

  private void recursiveGetNodes(Node node, Point point, int detailLevel, boolean inclParents,
                                 Collection<Node> result) {
    if (inclParents) {
      result.add(node);
    }
    final Node pCell = node.getSubCell(point);
    if (node.getLevel() == detailLevel - 1) {
      pCell.setLeaf();
      result.add(pCell);
    } else {
      recursiveGetNodes(pCell, point, detailLevel, inclParents, result);//tail call
    }
  }

  /**
   * Subclasses might override {@link #getNodes(com.spatial4j.core.shape.Shape, int, boolean)}
   * and check if the argument is a shape and if so, delegate
   * to this implementation, which calls {@link #getNode(com.spatial4j.core.shape.Point, int)} and
   * then calls {@link #getNode(String)} repeatedly if inclParents is true.
   */
  protected final List<Node> getNodesAltPoint(Point p, int detailLevel, boolean inclParents) {
    Node cell = getNode(p, detailLevel);
    if (!inclParents) {
      return Collections.singletonList(cell);
    }

    String endToken = cell.getTokenString();
    assert endToken.length() == detailLevel;
    List<Node> cells = new ArrayList<Node>(detailLevel);
    for (int i = 1; i < detailLevel; i++) {
      cells.add(getNode(endToken.substring(0, i)));
    }
    cells.add(cell);
    return cells;
  }

  /**
   * Will add the trailing leaf byte for leaves. This isn't particularly efficient.
   */
  public static List<String> nodesToTokenStrings(Collection<Node> nodes) {
    List<String> tokens = new ArrayList<String>((nodes.size()));
    for (Node node : nodes) {
      final String token = node.getTokenString();
      if (node.isLeaf()) {
        tokens.add(token + (char) Node.LEAF_BYTE);
      } else {
        tokens.add(token);
      }
    }
    return tokens;
  }
}
