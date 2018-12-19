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
package org.apache.lucene.geo;

import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.geo.GeoUtils.WindingOrder;
import org.apache.lucene.util.BitUtil;

import static org.apache.lucene.geo.GeoEncodingUtils.encodeLatitude;
import static org.apache.lucene.geo.GeoEncodingUtils.encodeLongitude;
import static org.apache.lucene.geo.GeoUtils.orient;

/**
 * Computes a triangular mesh tessellation for a given polygon.
 * <p>
 * This is inspired by mapbox's earcut algorithm (https://github.com/mapbox/earcut)
 * which is a modification to FIST (https://www.cosy.sbg.ac.at/~held/projects/triang/triang.html)
 * written by Martin Held, and ear clipping (https://www.geometrictools.com/Documentation/TriangulationByEarClipping.pdf)
 * written by David Eberly.
 * <p>
 * Notes:
 *   <ul>
 *     <li>Requires valid polygons:
 *       <ul>
 *         <li>No self intersections
 *         <li>Holes may only touch at one vertex
 *         <li>Polygon must have an area (e.g., no "line" boxes)
 *      <li>sensitive to overflow (e.g, subatomic values such as E-200 can cause unexpected behavior)
 *      </ul>
 *  </ul>
 * <p>
 * The code is a modified version of the javascript implementation provided by MapBox
 * under the following license:
 * <p>
 * ISC License
 * <p>
 * Copyright (c) 2016, Mapbox
 * <p>
 * Permission to use, copy, modify, and/or distribute this software for any purpose
 * with or without fee is hereby granted, provided that the above copyright notice
 * and this permission notice appear in all copies.
 * <p>
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES WITH'
 * REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND
 * FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY SPECIAL, DIRECT,
 * INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER RESULTING FROM LOSS
 * OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER
 * TORTIOUS ACTION, ARISING OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF
 * THIS SOFTWARE.
 *
 */
public final class XTessellator {
  // this is a dumb heuristic to control whether we cut over to sorted morton values
  private static final int VERTEX_THRESHOLD = 80;

  /** state of the tessellated split - avoids recursion */
  private enum State {
    INIT, CURE, SPLIT
  }

  // No Instance:
  private XTessellator() {}

  /** Produces an array of vertices representing the triangulated result set of the Points array */
  public static List<Triangle> tessellate(final Polygon polygon) {
    // Attempt to establish a doubly-linked list of the provided shell points (should be CCW, but this will correct);
    // then filter instances of intersections.
    Node outerNode = createDoublyLinkedList(polygon, 0, WindingOrder.CW);
    // If an outer node hasn't been detected, the shape is malformed. (must comply with OGC SFA specification)
    if(outerNode == null) {
      throw new IllegalArgumentException("Malformed shape detected in XTessellator!");
    }

    // Determine if the specified list of points contains holes
    if (polygon.numHoles() > 0) {
      // Eliminate the hole triangulation.
      outerNode = eliminateHoles(polygon, outerNode);
    }

    // If the shape crosses VERTEX_THRESHOLD, use z-order curve hashing:
    final boolean mortonOptimized;
    {
      int threshold = VERTEX_THRESHOLD - polygon.numPoints();
      for (int i = 0; threshold >= 0 && i < polygon.numHoles(); ++i) {
        threshold -= polygon.getHole(i).numPoints();
      }

      // Link polygon nodes in Z-Order
      mortonOptimized = threshold < 0;
      if (mortonOptimized == true) {
        sortByMorton(outerNode);
      }
    }
    // Calculate the tessellation using the doubly LinkedList.
    List<Triangle> result = earcutLinkedList(outerNode, new ArrayList<>(), State.INIT, mortonOptimized);
    if (result.size() == 0) {
      throw new IllegalArgumentException("Unable to Tessellate shape [" + polygon + "]. Possible malformed shape detected.");
    }

    return result;
  }

  /** Creates a circular doubly linked list using polygon points. The order is governed by the specified winding order */
  private static Node createDoublyLinkedList(final Polygon polygon, int startIndex, final WindingOrder windingOrder) {
    Node lastNode = null;
    // Link points into the circular doubly-linked list in the specified winding order
    if (windingOrder == polygon.getWindingOrder()) {
      for (int i = 0; i < polygon.numPoints(); ++i) {
        lastNode = insertNode(polygon, startIndex++, i, lastNode);
      }
    } else {
      for (int i = polygon.numPoints() - 1; i >= 0; --i) {
        lastNode = insertNode(polygon, startIndex++, i, lastNode);
      }
    }
    // if first and last node are the same then remove the end node and set lastNode to the start
    if (lastNode != null && isVertexEquals(lastNode, lastNode.next)) {
      removeNode(lastNode);
      lastNode = lastNode.next;
    }

    // Return the last node in the Doubly-Linked List
    return filterPoints(lastNode, null);
  }

  /** Links every hole into the outer loop, producing a single-ring polygon without holes. **/
  private static Node eliminateHoles(final Polygon polygon, Node outerNode) {
    // Define a list to hole a reference to each filtered hole list.
    final List<Node> holeList = new ArrayList<>();
    // Iterate through each array of hole vertices.
    Polygon[] holes = polygon.getHoles();
    int nodeIndex = polygon.numPoints();
    for(int i = 0; i < polygon.numHoles(); ++i) {
      // create the doubly-linked hole list
      Node list = createDoublyLinkedList(holes[i], nodeIndex, WindingOrder.CCW);
      if (list == list.next) {
        list.isSteiner = true;
      }
      // Determine if the resulting hole polygon was successful.
      if(list != null) {
        // Add the leftmost vertex of the hole.
        holeList.add(fetchLeftmost(list));
      }
      nodeIndex += holes[i].numPoints();
    }

    // Sort the hole vertices by x coordinate
    holeList.sort((Node pNodeA, Node pNodeB) ->
        pNodeA.getX() < pNodeB.getX() ? -1 : pNodeA.getX() == pNodeB.getX() ? 0 : 1);

    // Process holes from left to right.
    for(int i = 0; i < holeList.size(); ++i) {
      // Eliminate hole triangles from the result set
      final Node holeNode = holeList.get(i);
      eliminateHole(holeNode, outerNode);
      // Filter the new polygon.
      outerNode = filterPoints(outerNode, outerNode.next);
    }
    // Return a pointer to the list.
    return outerNode;
  }

  /** Finds a bridge between vertices that connects a hole with an outer ring, and links it */
  private static void eliminateHole(final Node holeNode, Node outerNode) {
    // Attempt to find a logical bridge between the HoleNode and OuterNode.
    outerNode = fetchHoleBridge(holeNode, outerNode);
    // Determine whether a hole bridge could be fetched.
    if(outerNode != null) {
      // Split the resulting polygon.
      Node node = splitPolygon(outerNode, holeNode);
      // Filter the split nodes.
      filterPoints(node, node.next);
    }
  }

  /**
   * David Eberly's algorithm for finding a bridge between a hole and outer polygon
   *
   * see: http://www.geometrictools.com/Documentation/TriangulationByEarClipping.pdf
   **/
  private static Node fetchHoleBridge(final Node holeNode, final Node outerNode) {
    Node p = outerNode;
    double qx = Double.NEGATIVE_INFINITY;
    final double hx = holeNode.getX();
    final double hy = holeNode.getY();
    Node connection = null;
    // 1. find a segment intersected by a ray from the hole's leftmost point to the left;
    // segment's endpoint with lesser x will be potential connection point
    {
      do {
        if (hy <= p.getY() && hy >= p.next.getY() && p.next.getY() != p.getY()) {
          final double x = p.getX() + (hy - p.getY()) * (p.next.getX() - p.getX()) / (p.next.getY() - p.getY());
          if (x <= hx && x > qx) {
            qx = x;
            if (x == hx) {
              if (hy == p.getY()) return p;
              if (hy == p.next.getY()) return p.next;
            }
            connection = p.getX() < p.next.getX() ? p : p.next;
          }
        }
        p = p.next;
      } while (p != outerNode);
    }

    if (connection == null) {
      return null;
    } else if (hx == qx) {
      return connection.previous;
    }

    // 2. look for points inside the triangle of hole point, segment intersection, and endpoint
    // its a valid connection iff there are no points found;
    // otherwise choose the point of the minimum angle with the ray as the connection point
    Node stop = connection;
    final double mx = connection.getX();
    final double my = connection.getY();
    double tanMin = Double.POSITIVE_INFINITY;
    double tan;
    p = connection.next;
    {
      while (p != stop) {
        if (hx >= p.getX() && p.getX() >= mx && hx != p.getX()
            && pointInEar(p.getX(), p.getY(), hy < my ? hx : qx, hy, mx, my, hy < my ? qx : hx, hy)) {
          tan = Math.abs(hy - p.getY()) / (hx - p.getX()); // tangential
          if ((tan < tanMin || (tan == tanMin && p.getX() > connection.getX())) && isLocallyInside(p, holeNode)) {
            connection = p;
            tanMin = tan;
          }
        }
        p = p.next;
      }
    }

    return connection;
  }

  /** Finds the left-most hole of a polygon ring. **/
  private static Node fetchLeftmost(final Node start) {
    Node node = start;
    Node leftMost = start;
    do {
      // Determine if the current node possesses a lesser X position.
      if (node.getX() < leftMost.getX()) {
        // Maintain a reference to this Node.
        leftMost = node;
      }
      // Progress the search to the next node in the doubly-linked list.
      node = node.next;
    } while (node != start);

    // Return the node with the smallest X value.
    return leftMost;
  }

  /** Main ear slicing loop which triangulates the vertices of a polygon, provided as a doubly-linked list. **/
  private static List<Triangle> earcutLinkedList(Node currEar, final List<Triangle> tessellation,
                                                       State state, final boolean mortonOptimized) {
    earcut : do {
      if (currEar == null || currEar.previous == currEar.next) {
        return tessellation;
      }

      Node stop = currEar;
      Node prevNode;
      Node nextNode;

      // Iteratively slice ears
      do {
        prevNode = currEar.previous;
        nextNode = currEar.next;
        // Determine whether the current triangle must be cut off.
        final boolean isReflex = area(prevNode.getX(), prevNode.getY(), currEar.getX(), currEar.getY(),
            nextNode.getX(), nextNode.getY()) >= 0;
        if (isReflex == false && isEar(currEar, mortonOptimized) == true) {
          // Return the triangulated data
          tessellation.add(new Triangle(prevNode, currEar, nextNode));
          // Remove the ear node.
          removeNode(currEar);

          // Skipping to the next node leaves fewer slither triangles.
          currEar = nextNode.next;
          stop = nextNode.next;
          continue;
        }
        currEar = nextNode;

        // If the whole polygon has been iterated over and no more ears can be found.
        if (currEar == stop) {
          switch (state) {
            case INIT:
              // try filtering points and slicing again
              currEar = filterPoints(currEar, null);
              state = State.CURE;
              continue earcut;
            case CURE:
              // if this didn't work, try curing all small self-intersections locally
              currEar = cureLocalIntersections(currEar, tessellation);
              state = State.SPLIT;
              continue earcut;
            case SPLIT:
              // as a last resort, try splitting the remaining polygon into two
              if (splitEarcut(currEar, tessellation, mortonOptimized) == false) {
                //we could not process all points. Tessellation failed
                tessellation.clear();
              }
              break;
          }
          break;
        }
      } while (currEar.previous != currEar.next);
      break;
    } while (true);
    // Return the calculated tessellation
    return tessellation;
  }

  /** Determines whether a polygon node forms a valid ear with adjacent nodes. **/
  private static boolean isEar(final Node ear, final boolean mortonOptimized) {
    if (mortonOptimized == true) {
      return mortonIsEar(ear);
    }

    // make sure there aren't other points inside the potential ear
    Node node = ear.next.next;
    while (node != ear.previous) {
      if (pointInEar(node.getX(), node.getY(), ear.previous.getX(), ear.previous.getY(), ear.getX(), ear.getY(),
          ear.next.getX(), ear.next.getY())
          && area(node.previous.getX(), node.previous.getY(), node.getX(), node.getY(),
          node.next.getX(), node.next.getY()) >= 0) {
        return false;
      }
      node = node.next;
    }
    return true;
  }

  /** Uses morton code for speed to determine whether or a polygon node forms a valid ear w/ adjacent nodes */
  private static boolean mortonIsEar(final Node ear) {
    // triangle bbox (flip the bits so negative encoded values are < positive encoded values)
    int minTX = StrictMath.min(StrictMath.min(ear.previous.x, ear.x), ear.next.x) ^ 0x80000000;
    int minTY = StrictMath.min(StrictMath.min(ear.previous.y, ear.y), ear.next.y) ^ 0x80000000;
    int maxTX = StrictMath.max(StrictMath.max(ear.previous.x, ear.x), ear.next.x) ^ 0x80000000;
    int maxTY = StrictMath.max(StrictMath.max(ear.previous.y, ear.y), ear.next.y) ^ 0x80000000;

    // z-order range for the current triangle bbox;
    long minZ = BitUtil.interleave(minTX, minTY);
    long maxZ = BitUtil.interleave(maxTX, maxTY);

    // now make sure we don't have other points inside the potential ear;

    // look for points inside the triangle in both directions
    Node p = ear.previousZ;
    Node n = ear.nextZ;
    while (p != null && Long.compareUnsigned(p.morton, minZ) >= 0
        && n != null && Long.compareUnsigned(n.morton, maxZ) <= 0) {
      if (p.idx != ear.previous.idx && p.idx != ear.next.idx &&
          pointInEar(p.getX(), p.getY(), ear.previous.getX(), ear.previous.getY(), ear.getX(), ear.getY(),
              ear.next.getX(), ear.next.getY()) &&
          area(p.previous.getX(), p.previous.getY(), p.getX(), p.getY(), p.next.getX(), p.next.getY()) >= 0) return false;
      p = p.previousZ;

      if (n.idx != ear.previous.idx && n.idx != ear.next.idx &&
          pointInEar(n.getX(), n.getY(), ear.previous.getX(), ear.previous.getY(), ear.getX(), ear.getY(),
              ear.next.getX(), ear.next.getY()) &&
          area(n.previous.getX(), n.previous.getY(), n.getX(), n.getY(), n.next.getX(), n.next.getY()) >= 0) return false;
      n = n.nextZ;
    }

    // first look for points inside the triangle in decreasing z-order
    while (p != null && Long.compareUnsigned(p.morton, minZ) >= 0) {
      if (p.idx != ear.previous.idx && p.idx != ear.next.idx
            && pointInEar(p.getX(), p.getY(), ear.previous.getX(), ear.previous.getY(), ear.getX(), ear.getY(),
          ear.next.getX(), ear.next.getY())
            && area(p.previous.getX(), p.previous.getY(), p.getX(), p.getY(), p.next.getX(), p.next.getY()) >= 0) {
          return false;
        }
      p = p.previousZ;
    }
    // then look for points in increasing z-order
    while (n != null &&
        Long.compareUnsigned(n.morton, maxZ) <= 0) {
        if (n.idx != ear.previous.idx && n.idx != ear.next.idx
            && pointInEar(n.getX(), n.getY(), ear.previous.getX(), ear.previous.getY(), ear.getX(), ear.getY(),
            ear.next.getX(), ear.next.getY())
            && area(n.previous.getX(), n.previous.getY(), n.getX(), n.getY(), n.next.getX(), n.next.getY()) >= 0) {
          return false;
        }
      n = n.nextZ;
    }
    return true;
  }

  /** Iterate through all polygon nodes and remove small local self-intersections **/
  private static Node cureLocalIntersections(Node startNode, final List<Triangle> tessellation) {
    Node node = startNode;
    Node nextNode;
    do {
      nextNode = node.next;
      Node a = node.previous;
      Node b = nextNode.next;

      // a self-intersection where edge (v[i-1],v[i]) intersects (v[i+1],v[i+2])
      if (isVertexEquals(a, b) == false
          && isIntersectingPolygon(a, a.getX(), a.getY(), b.getX(), b.getY()) == false
          && linesIntersect(a.getX(), a.getY(), node.getX(), node.getY(), nextNode.getX(), nextNode.getY(), b.getX(), b.getY())
          && isLocallyInside(a, b) && isLocallyInside(b, a)) {
        // Return the triangulated vertices to the tessellation
        tessellation.add(new Triangle(a, node, b));

        // remove two nodes involved
        removeNode(node);
        removeNode(node.next);
        node = startNode = b;
      }
      node = node.next;
    } while (node != startNode);

    return node;
  }

  /** Attempt to split a polygon and independently triangulate each side. Return true if the polygon was splitted **/
  private static boolean splitEarcut(final Node start, final List<Triangle> tessellation, final boolean mortonIndexed) {
    // Search for a valid diagonal that divides the polygon into two.
    Node searchNode = start;
    Node nextNode;
    do {
      nextNode = searchNode.next;
      Node diagonal = nextNode.next;
      while (diagonal != searchNode.previous) {
        if(isValidDiagonal(searchNode, diagonal)) {
          // Split the polygon into two at the point of the diagonal
          Node splitNode = splitPolygon(searchNode, diagonal);
          // Filter the resulting polygon.
          searchNode = filterPoints(searchNode, searchNode.next);
          splitNode  = filterPoints(splitNode, splitNode.next);
          // Attempt to earcut both of the resulting polygons
          if (mortonIndexed) {
            sortByMortonWithReset(searchNode);
            sortByMortonWithReset(splitNode);
          }
          earcutLinkedList(searchNode, tessellation, State.INIT, mortonIndexed);
          earcutLinkedList(splitNode,  tessellation, State.INIT, mortonIndexed);
          // Finish the iterative search
          return true;
        }
        diagonal = diagonal.next;
      }
      searchNode = searchNode.next;
    } while (searchNode != start);
    return false;
  }

  /** Links two polygon vertices using a bridge. **/
  private static Node splitPolygon(final Node a, final Node b) {
    final Node a2 = new Node(a);
    final Node b2 = new Node(b);
    final Node an = a.next;
    final Node bp = b.previous;

    a.next = b;
    a.nextZ = b;
    b.previous = a;
    b.previousZ = a;
    a2.next = an;
    a2.nextZ = an;
    an.previous = a2;
    an.previousZ = a2;
    b2.next = a2;
    b2.nextZ = a2;
    a2.previous = b2;
    a2.previousZ = b2;
    bp.next = b2;
    bp.nextZ = b2;

    return b2;
  }

  /** Determines whether a diagonal between two polygon nodes lies within a polygon interior.
   * (This determines the validity of the ray.) **/
  private static boolean isValidDiagonal(final Node a, final Node b) {
    return a.next.idx != b.idx && a.previous.idx != b.idx
        && isIntersectingPolygon(a, a.getX(), a.getY(), b.getX(), b.getY()) == false
        && isLocallyInside(a, b) && isLocallyInside(b, a)
        && middleInsert(a, a.getX(), a.getY(), b.getX(), b.getY());
  }

  private static boolean isLocallyInside(final Node a, final Node b) {
    // if a is cw
    if (area(a.previous.getX(), a.previous.getY(), a.getX(), a.getY(), a.next.getX(), a.next.getY()) < 0) {
      return area(a.getX(), a.getY(), b.getX(), b.getY(), a.next.getX(), a.next.getY()) >= 0
          && area(a.getX(), a.getY(), a.previous.getX(), a.previous.getY(), b.getX(), b.getY()) >= 0;
    }
    // ccw
    return area(a.getX(), a.getY(), b.getX(), b.getY(), a.previous.getX(), a.previous.getY()) < 0
        || area(a.getX(), a.getY(), a.next.getX(), a.next.getY(), b.getX(), b.getY()) < 0;
  }

  /** Determine whether the middle point of a polygon diagonal is contained within the polygon */
  private static boolean middleInsert(final Node start, final double x0, final double y0,
                                            final double x1, final double y1) {
    Node node = start;
    Node nextNode;
    boolean lIsInside = false;
    final double lDx = (x0 + x1) / 2.0f;
    final double lDy = (y0 + y1) / 2.0f;
    do {
      nextNode = node.next;
      if (node.getY() > lDy != nextNode.getY() > lDy &&
          lDx < (nextNode.getX() - node.getX()) * (lDy - node.getY()) / (nextNode.getY() - node.getY()) + node.getX()) {
        lIsInside = !lIsInside;
      }
      node = node.next;
    } while (node != start);
    return lIsInside;
  }

  /** Determines if the diagonal of a polygon is intersecting with any polygon elements. **/
  private static boolean isIntersectingPolygon(final Node start, final double x0, final double y0,
                                                     final double x1, final double y1) {
    Node node = start;
    Node nextNode;
    do {
      nextNode = node.next;
      if(isVertexEquals(node, x0, y0) == false && isVertexEquals(node, x1, y1) == false) {
        if (linesIntersect(node.getX(), node.getY(), nextNode.getX(), nextNode.getY(), x0, y0, x1, y1)) {
          return true;
        }
      }
      node = nextNode;
    } while (node != start);

    return false;
  }

  /** Determines whether two line segments intersect. **/
  public static boolean linesIntersect(final double aX0, final double aY0, final double aX1, final double aY1,
                                             final double bX0, final double bY0, final double bX1, final double bY1) {
    return (area(aX0, aY0, aX1, aY1, bX0, bY0) > 0) != (area(aX0, aY0, aX1, aY1, bX1, bY1) > 0)
        && (area(bX0, bY0, bX1, bY1, aX0, aY0) > 0) != (area(bX0, bY0, bX1, bY1, aX1, aY1) > 0);
  }

  /** Interlinks polygon nodes in Z-Order. It reset the values on the z values**/
  private static void sortByMortonWithReset(Node start) {
    Node next = start;
    do {
      next.previousZ = next.previous;
      next.nextZ = next.next;
      next = next.next;
    } while (next != start);
    sortByMorton(start);
  }

  /** Interlinks polygon nodes in Z-Order. **/
  private static void sortByMorton(Node start) {
    start.previousZ.nextZ = null;
    start.previousZ = null;
    // Sort the generated ring using Z ordering.
    tathamSort(start);
  }

  /**
   * Simon Tatham's doubly-linked list O(n log n) mergesort
   * see: http://www.chiark.greenend.org.uk/~sgtatham/algorithms/listsort.html
   **/
  private static void tathamSort(Node list) {
    Node p, q, e, tail;
    int i, numMerges, pSize, qSize;
    int inSize = 1;

    if (list == null) {
      return;
    }

    do {
      p = list;
      list = null;
      tail = null;
      // count number of merges in this pass
      numMerges = 0;

      while(p != null) {
        ++numMerges;
        // step 'insize' places along from p
        q = p;
        for (i = 0, pSize = 0; i < inSize && q != null; ++i, ++pSize, q = q.nextZ);
        // if q hasn't fallen off end, we have two lists to merge
        qSize = inSize;

        // now we have two lists; merge
        while (pSize > 0 || (qSize > 0 && q != null)) {
          if (pSize != 0 && (qSize == 0 || q == null || Long.compareUnsigned(p.morton, q.morton) <= 0)) {
            e = p;
            p = p.nextZ;
            --pSize;
          } else {
            e = q;
            q = q.nextZ;
            --qSize;
          }

          if (tail != null) {
            tail.nextZ = e;
          } else {
            list = e;
          }
          // maintain reverse pointers
          e.previousZ = tail;
          tail = e;
        }
        // now p has stepped 'insize' places along, and q has too
        p = q;
      }

      tail.nextZ = null;
      inSize *= 2;
    } while (numMerges > 1);
  }

  /** Eliminate colinear/duplicate points from the doubly linked list */
  private static Node filterPoints(final Node start, Node end) {
    if (start == null) {
      return start;
    }

    if(end == null) {
      end = start;
    }

    Node node = start;
    Node nextNode;
    Node prevNode;
    boolean continueIteration;

    do {
      continueIteration = false;
      nextNode = node.next;
      prevNode = node.previous;
      if (node.isSteiner == false && isVertexEquals(node, nextNode)
          || area(prevNode.getX(), prevNode.getY(), node.getX(), node.getY(), nextNode.getX(), nextNode.getY()) == 0) {
        // Remove the node
        removeNode(node);
        node = end = prevNode;

        if (node == nextNode) {
          break;
        }
        continueIteration = true;
      } else {
        node = nextNode;
      }
    } while (continueIteration || node != end);
    return end;
  }

  /** Creates a node and optionally links it with a previous node in a circular doubly-linked list */
  private static Node insertNode(final Polygon polygon, int index, int vertexIndex, final Node lastNode) {
    final Node node = new Node(polygon, index, vertexIndex);
    if(lastNode == null) {
      node.previous = node;
      node.previousZ = node;
      node.next = node;
      node.nextZ = node;
    } else {
      node.next = lastNode.next;
      node.nextZ = lastNode.next;
      node.previous = lastNode;
      node.previousZ = lastNode;
      lastNode.next.previous = node;
      lastNode.nextZ.previousZ = node;
      lastNode.next = node;
      lastNode.nextZ = node;
    }
    return node;
  }

  /** Removes a node from the doubly linked list */
  private static void removeNode(Node node) {
    node.next.previous = node.previous;
    node.previous.next = node.next;

    if (node.previousZ != null) {
      node.previousZ.nextZ = node.nextZ;
    }
    if (node.nextZ != null) {
      node.nextZ.previousZ = node.previousZ;
    }
  }

  /** Determines if two point vertices are equal. **/
  private static boolean isVertexEquals(final Node a, final Node b) {
    return isVertexEquals(a, b.getX(), b.getY());
  }

  /** Determines if two point vertices are equal. **/
  private static boolean isVertexEquals(final Node a, final double x, final  double y) {
    return a.getX() == x && a.getY() == y;
  }

  /** Compute signed area of triangle */
  private static double area(final double aX, final double aY, final double bX, final double bY,
                             final double cX, final double cY) {
    return (bY - aY) * (cX - bX) - (bX - aX) * (cY - bY);
  }

  /** Compute whether point is in a candidate ear */
  private static boolean pointInEar(final double x, final double y, final double ax, final double ay,
                                    final double bx, final double by, final double cx, final double cy) {
    return (cx - x) * (ay - y) - (ax - x) * (cy - y) >= 0 &&
           (ax - x) * (by - y) - (bx - x) * (ay - y) >= 0 &&
           (bx - x) * (cy - y) - (cx - x) * (by - y) >= 0;
  }

  /** compute whether the given x, y point is in a triangle; uses the winding order method */
  public static boolean pointInTriangle (double x, double y, double ax, double ay, double bx, double by, double cx, double cy) {
    int a = orient(x, y, ax, ay, bx, by);
    int b = orient(x, y, bx, by, cx, cy);
    if (a == 0 || b == 0 || a < 0 == b < 0) {
      int c = orient(x, y, cx, cy, ax, ay);
      return c == 0 || (c < 0 == (b < 0 || a < 0));
    }
    return false;
  }

  /** Brute force compute if a point is in the polygon by traversing entire triangulation
   * todo: speed this up using either binary tree or prefix coding (filtering by bounding box of triangle)
   **/
  public static boolean pointInPolygon(final List<Triangle> tessellation, double lat, double lon) {
    // each triangle
    for (int i = 0; i < tessellation.size(); ++i) {
      if (tessellation.get(i).containsPoint(lat, lon)) {
        return true;
      }
    }
    return false;
  }

  /** Circular Doubly-linked list used for polygon coordinates */
  protected static class Node {
    // node index in the linked list
    private final int idx;
    // vertex index in the polygon
    private final int vrtxIdx;
    // reference to the polygon for lat/lon values
    private final Polygon polygon;
    // encoded x value
    private final int x;
    // encoded y value
    private final int y;
    // morton code for sorting
    private final long morton;

    // previous node
    private Node previous;
    // next node
    private Node next;
    // previous z node
    private Node previousZ;
    // next z node
    private Node nextZ;
    // triangle center
    private boolean isSteiner = false;

    protected Node(final Polygon polygon, final int index, final int vertexIndex) {
      this.idx = index;
      this.vrtxIdx = vertexIndex;
      this.polygon = polygon;
      this.y = encodeLatitude(polygon.getPolyLat(vrtxIdx));
      this.x = encodeLongitude(polygon.getPolyLon(vrtxIdx));
      this.morton = BitUtil.interleave(x ^ 0x80000000, y ^ 0x80000000);
      this.previous = null;
      this.next = null;
      this.previousZ = null;
      this.nextZ = null;
    }

    /** simple deep copy constructor */
    protected Node(Node other) {
      this.idx = other.idx;
      this.vrtxIdx = other.vrtxIdx;
      this.polygon = other.polygon;
      this.morton = other.morton;
      this.x = other.x;
      this.y = other.y;
      this.previous = other.previous;
      this.next = other.next;
      this.previousZ = other.previousZ;
      this.nextZ = other.nextZ;
      this.isSteiner = other.isSteiner;
    }

    /** get the x value */
    public final double getX() {
      return polygon.getPolyLon(vrtxIdx);
    }

    /** get the y value */
    public final double getY() {
      return polygon.getPolyLat(vrtxIdx);
    }

    /** get the longitude value */
    public final double getLon() {
      return polygon.getPolyLon(vrtxIdx);
    }

    /** get the latitude value */
    public final double getLat() {
      return polygon.getPolyLat(vrtxIdx);
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      if (this.previous == null)
        builder.append("||-");
      else
        builder.append(this.previous.idx + " <- ");
      builder.append(this.idx);
      if (this.next == null)
        builder.append(" -||");
      else
        builder.append(" -> " + this.next.idx);
      return builder.toString();
    }
  }

  /** Triangle in the tessellated mesh */
  public static final class Triangle {
    Node[] vertex;

    protected Triangle(Node a, Node b, Node c) {
      this.vertex = new Node[] {a, b, c};
    }

    /** get quantized x value for the given vertex */
    public int getEncodedX(int vertex) {
      return this.vertex[vertex].x;
    }

    /** get quantized y value for the given vertex */
    public int getEncodedY(int vertex) {
      return this.vertex[vertex].y;
    }

    /** get latitude value for the given vertex */
    public double getLat(int vertex) {
      return this.vertex[vertex].getLat();
    }

    /** get longitude value for the given vertex */
    public double getLon(int vertex) {
      return this.vertex[vertex].getLon();
    }

    /** utility method to compute whether the point is in the triangle */
    protected boolean containsPoint(double lat, double lon) {
      return pointInTriangle(lon, lat,
          vertex[0].getLon(), vertex[0].getLat(),
          vertex[1].getLon(), vertex[1].getLat(),
          vertex[2].getLon(), vertex[2].getLat());
    }

    /** pretty print the triangle vertices */
    public String toString() {
      String result = vertex[0].x + ", " + vertex[0].y + " " +
                      vertex[1].x + ", " + vertex[1].y + " " +
                      vertex[2].x + ", " + vertex[2].y;
      return result;
    }
  }
}
