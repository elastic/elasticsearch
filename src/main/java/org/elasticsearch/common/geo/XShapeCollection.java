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

package org.elasticsearch.common.geo;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;
import com.spatial4j.core.shape.ShapeCollection;

import java.util.Collection;
import java.util.List;

/**
 * Overrides bounding box logic in ShapeCollection base class to comply with
 * OGC OpenGIS Abstract Specification: An Object Model for Interoperable Geoprocessing.
 *
 * NOTE: This algorithm is O(N) and can possibly be improved O(log n) using an internal R*-Tree
 * data structure for a collection of bounding boxes
 */
public class XShapeCollection<S extends Shape> extends ShapeCollection<S> {

  public XShapeCollection(List<S> shapes, SpatialContext ctx) {
    super(shapes, ctx);
  }

  @Override
  protected Rectangle computeBoundingBox(Collection<? extends Shape> shapes, SpatialContext ctx) {
    Rectangle retBox = shapes.iterator().next().getBoundingBox();
    for (Shape geom : shapes) {
      retBox = expandBBox(retBox, geom.getBoundingBox());
    }
    return retBox;
  }

  /**
   * Spatial4J shapes have no knowledge of directed edges. For this reason, a bounding box
   * that wraps the dateline can have a min longitude that is mathematically > than the
   * Rectangles' minX value.  This is an issue for geometric collections (e.g., MultiPolygon
   * and ShapeCollection) Until geometry logic can be cleaned up in Spatial4J, ES provides
   * the following expansion algorithm for GeometryCollections
   */
  private Rectangle expandBBox(Rectangle bbox, Rectangle expand) {
    if (bbox.equals(expand) || bbox.equals(SpatialContext.GEO.getWorldBounds())) {
      return bbox;
    }

    double minX = bbox.getMinX();
    double eMinX = expand.getMinX();
    double maxX = bbox.getMaxX();
    double eMaxX = expand.getMaxX();
    double minY = bbox.getMinY();
    double eMinY = expand.getMinY();
    double maxY = bbox.getMaxY();
    double eMaxY = expand.getMaxY();

    bbox.reset(Math.min(Math.min(minX, maxX), Math.min(eMinX, eMaxX)),
            Math.max(Math.max(minX, maxX), Math.max(eMinX, eMaxX)),
            Math.min(Math.min(minY, maxY), Math.min(eMinY, eMaxY)),
            Math.max(Math.max(minY, maxY), Math.max(eMinY, eMaxY)));

    return bbox;
  }
}
