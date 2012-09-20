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
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.SpatialRelation;

import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

/**
 * @lucene.experimental
 */
public class QuadPrefixTree extends SpatialPrefixTree {

  public static final int MAX_LEVELS_POSSIBLE = 50;//not really sure how big this should be

  public static final int DEFAULT_MAX_LEVELS = 12;
  private final double xmin;
  private final double xmax;
  private final double ymin;
  private final double ymax;
  private final double xmid;
  private final double ymid;

  private final double gridW;
  public final double gridH;

  final double[] levelW;
  final double[] levelH;
  final int[] levelS; // side
  final int[] levelN; // number

  public QuadPrefixTree(
      SpatialContext ctx, Rectangle bounds, int maxLevels) {
    super(ctx, maxLevels);
    this.xmin = bounds.getMinX();
    this.xmax = bounds.getMaxX();
    this.ymin = bounds.getMinY();
    this.ymax = bounds.getMaxY();

    levelW = new double[maxLevels];
    levelH = new double[maxLevels];
    levelS = new int[maxLevels];
    levelN = new int[maxLevels];

    gridW = xmax - xmin;
    gridH = ymax - ymin;
    this.xmid = xmin + gridW / 2.0;
    this.ymid = ymin + gridH / 2.0;
    levelW[0] = gridW / 2.0;
    levelH[0] = gridH / 2.0;
    levelS[0] = 2;
    levelN[0] = 4;

    for (int i = 1; i < levelW.length; i++) {
      levelW[i] = levelW[i - 1] / 2.0;
      levelH[i] = levelH[i - 1] / 2.0;
      levelS[i] = levelS[i - 1] * 2;
      levelN[i] = levelN[i - 1] * 4;
    }
  }

  public QuadPrefixTree(SpatialContext ctx) {
    this(ctx, DEFAULT_MAX_LEVELS);
  }

  public QuadPrefixTree(
      SpatialContext ctx, int maxLevels) {
    this(ctx, ctx.getWorldBounds(), maxLevels);
  }

  public void printInfo(PrintStream out) {
    NumberFormat nf = NumberFormat.getNumberInstance(Locale.ROOT);
    nf.setMaximumFractionDigits(5);
    nf.setMinimumFractionDigits(5);
    nf.setMinimumIntegerDigits(3);

    for (int i = 0; i < maxLevels; i++) {
      out.println(i + "]\t" + nf.format(levelW[i]) + "\t" + nf.format(levelH[i]) + "\t" +
          levelS[i] + "\t" + (levelS[i] * levelS[i]));
    }
  }

  @Override
  public int getLevelForDistance(double dist) {
    if (dist == 0)//short circuit
      return maxLevels;
    for (int i = 0; i < maxLevels - 1; i++) {
      //note: level[i] is actually a lookup for level i+1
      if (dist > levelW[i] && dist > levelH[i]) {
        return i + 1;
      }
    }
    return maxLevels;
  }

  @Override
  public Node getNode(Point p, int level) {
    List<Node> cells = new ArrayList<Node>(1);
    build(xmid, ymid, 0, cells, new StringBuilder(), ctx.makePoint(p.getX(), p.getY()), level);
    return cells.get(0);//note cells could be longer if p on edge
  }

  @Override
  public Node getNode(String token) {
    return new QuadCell(token);
  }

  @Override
  public Node getNode(byte[] bytes, int offset, int len) {
    return new QuadCell(bytes, offset, len);
  }

  @Override //for performance
  public List<Node> getNodes(Shape shape, int detailLevel, boolean inclParents) {
    if (shape instanceof Point)
      return super.getNodesAltPoint((Point) shape, detailLevel, inclParents);
    else
      return super.getNodes(shape, detailLevel, inclParents);
  }

  private void build(
      double x,
      double y,
      int level,
      List<Node> matches,
      StringBuilder str,
      Shape shape,
      int maxLevel) {
    assert str.length() == level;
    double w = levelW[level] / 2;
    double h = levelH[level] / 2;

    // Z-Order
    // http://en.wikipedia.org/wiki/Z-order_%28curve%29
    checkBattenberg('A', x - w, y + h, level, matches, str, shape, maxLevel);
    checkBattenberg('B', x + w, y + h, level, matches, str, shape, maxLevel);
    checkBattenberg('C', x - w, y - h, level, matches, str, shape, maxLevel);
    checkBattenberg('D', x + w, y - h, level, matches, str, shape, maxLevel);

    // possibly consider hilbert curve
    // http://en.wikipedia.org/wiki/Hilbert_curve
    // http://blog.notdot.net/2009/11/Damn-Cool-Algorithms-Spatial-indexing-with-Quadtrees-and-Hilbert-Curves
    // if we actually use the range property in the query, this could be useful
  }

  private void checkBattenberg(
      char c,
      double cx,
      double cy,
      int level,
      List<Node> matches,
      StringBuilder str,
      Shape shape,
      int maxLevel) {
    assert str.length() == level;
    double w = levelW[level] / 2;
    double h = levelH[level] / 2;

    int strlen = str.length();
    Rectangle rectangle = ctx.makeRectangle(cx - w, cx + w, cy - h, cy + h);
    SpatialRelation v = shape.relate(rectangle);
    if (SpatialRelation.CONTAINS == v) {
      str.append(c);
      //str.append(SpatialPrefixGrid.COVER);
      matches.add(new QuadCell(str.toString(), v.transpose()));
    } else if (SpatialRelation.DISJOINT == v) {
      // nothing
    } else { // SpatialRelation.WITHIN, SpatialRelation.INTERSECTS
      str.append(c);

      int nextLevel = level + 1;
      if (nextLevel >= maxLevel) {
        //str.append(SpatialPrefixGrid.INTERSECTS);
        matches.add(new QuadCell(str.toString(), v.transpose()));
      } else {
        build(cx, cy, nextLevel, matches, str, shape, maxLevel);
      }
    }
    str.setLength(strlen);
  }

  class QuadCell extends Node {

    public QuadCell(String token) {
      super(QuadPrefixTree.this, token);
    }

    public QuadCell(String token, SpatialRelation shapeRel) {
      super(QuadPrefixTree.this, token);
      this.shapeRel = shapeRel;
    }

    QuadCell(byte[] bytes, int off, int len) {
      super(QuadPrefixTree.this, bytes, off, len);
    }

    @Override
    public void reset(byte[] bytes, int off, int len) {
      super.reset(bytes, off, len);
      shape = null;
    }

    @Override
    public Collection<Node> getSubCells() {
      List<Node> cells = new ArrayList<Node>(4);
      cells.add(new QuadCell(getTokenString() + "A"));
      cells.add(new QuadCell(getTokenString() + "B"));
      cells.add(new QuadCell(getTokenString() + "C"));
      cells.add(new QuadCell(getTokenString() + "D"));
      return cells;
    }

    @Override
    public int getSubCellsSize() {
      return 4;
    }

    @Override
    public Node getSubCell(Point p) {
      return QuadPrefixTree.this.getNode(p, getLevel() + 1);//not performant!
    }

    private Shape shape;//cache

    @Override
    public Shape getShape() {
      if (shape == null)
        shape = makeShape();
      return shape;
    }

    private Rectangle makeShape() {
      String token = getTokenString();
      double xmin = QuadPrefixTree.this.xmin;
      double ymin = QuadPrefixTree.this.ymin;

      for (int i = 0; i < token.length(); i++) {
        char c = token.charAt(i);
        if ('A' == c || 'a' == c) {
          ymin += levelH[i];
        } else if ('B' == c || 'b' == c) {
          xmin += levelW[i];
          ymin += levelH[i];
        } else if ('C' == c || 'c' == c) {
          // nothing really
        } else if ('D' == c || 'd' == c) {
          xmin += levelW[i];
        } else {
          throw new RuntimeException("unexpected char: " + c);
        }
      }
      int len = token.length();
      double width, height;
      if (len > 0) {
        width = levelW[len - 1];
        height = levelH[len - 1];
      } else {
        width = gridW;
        height = gridH;
      }
      return ctx.makeRectangle(xmin, xmin + width, ymin, ymin + height);
    }
  }//QuadCell
}
