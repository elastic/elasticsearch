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

package org.elasticsearch.common.geo.builders;

import com.google.common.collect.Sets;
import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.shape.Shape;
import com.vividsolutions.jts.geom.*;
import org.apache.commons.lang3.tuple.Pair;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;

/**
 * The {@link BasePolygonBuilder} implements the groundwork to create polygons. This contains
 * Methods to wrap polygons at the dateline and building shapes from the data held by the
 * builder.
 * Since this Builder can be embedded to other builders (i.e. {@link MultiPolygonBuilder}) 
 * the class of the embedding builder is given by the generic argument <code>E</code>  

 * @param <E> type of the embedding class
 */
public abstract class BasePolygonBuilder<E extends BasePolygonBuilder<E>> extends ShapeBuilder {

    public static final GeoShapeType TYPE = GeoShapeType.POLYGON;

    // Linear ring defining the shell of the polygon
    protected Ring<E> shell; 

    // List of linear rings defining the holes of the polygon 
    protected final ArrayList<BaseLineStringBuilder<?>> holes = new ArrayList<>();

    public BasePolygonBuilder(Orientation orientation) {
        super(orientation);
    }

    @SuppressWarnings("unchecked")
    private E thisRef() {
        return (E)this;
    }
    
    public E point(double longitude, double latitude) {
        shell.point(longitude, latitude);
        return thisRef();
    }

    /**
     * Add a point to the shell of the polygon
     * @param coordinate coordinate of the new point
     * @return this
     */
    public E point(Coordinate coordinate) {
        shell.point(coordinate);
        return thisRef();
    }

    /**
     * Add a array of points to the shell of the polygon
     * @param coordinates coordinates of the new points to add
     * @return this
     */
    public E points(Coordinate...coordinates) {
        shell.points(coordinates);
        return thisRef();
    }

    /**
     * Add a new hole to the polygon
     * @param hole linear ring defining the hole
     * @return this
     */
    public E hole(BaseLineStringBuilder<?> hole) {
        holes.add(hole);
        return thisRef();
    }

    /**
     * build new hole to the polygon
     * @param hole linear ring defining the hole
     * @return this
     */
    public Ring<E> hole() {
        Ring<E> hole = new Ring<>(thisRef());
        this.holes.add(hole);
        return hole;
    }

    /**
     * Close the shell of the polygon
     * @return parent
     */
    public ShapeBuilder close() {
        return shell.close();
    }

    /**
     * Validates only 1 vertex is tangential (shared) between the interior and exterior of a polygon
     */
    protected void validateHole(BaseLineStringBuilder shell, BaseLineStringBuilder hole) {
        HashSet exterior = Sets.newHashSet(shell.points);
        HashSet interior = Sets.newHashSet(hole.points);
        exterior.retainAll(interior);
        if (exterior.size() >= 2) {
            throw new InvalidShapeException("Invalid polygon, interior cannot share more than one point with the exterior");
        }
    }

    /**
     * The coordinates setup by the builder will be assembled to a polygon. The result will consist of
     * a set of polygons. Each of these components holds a list of linestrings defining the polygon: the
     * first set of coordinates will be used as the shell of the polygon. The others are defined to holes
     * within the polygon.
     * This Method also wraps the polygons at the dateline. In order to this fact the result may
     * contains more polygons and less holes than defined in the builder it self.
     * 
     * @return coordinates of the polygon
     */
    public Coordinate[][][] coordinates() {
        int numEdges = shell.points.size()-1; // Last point is repeated 
        for (int i = 0; i < holes.size(); i++) {
            numEdges += holes.get(i).points.size()-1;
            validateHole(shell, this.holes.get(i));
        }

        Edge[] edges = new Edge[numEdges];
        Edge[] holeComponents = new Edge[holes.size()];
        int offset = createEdges(0, orientation, shell, null, edges, 0);
        for (int i = 0; i < holes.size(); i++) {
            int length = createEdges(i+1, orientation, shell, this.holes.get(i), edges, offset);
            holeComponents[i] = edges[offset];
            offset += length;
        }

        int numHoles = holeComponents.length;

        numHoles = merge(edges, 0, intersections(+DATELINE, edges), holeComponents, numHoles);
        numHoles = merge(edges, 0, intersections(-DATELINE, edges), holeComponents, numHoles);

        return compose(edges, holeComponents, numHoles);
    }

    @Override
    public Shape build() {
        return jtsGeometry(buildGeometry(FACTORY, wrapdateline));
    }

    protected XContentBuilder coordinatesArray(XContentBuilder builder, Params params) throws IOException {
        shell.coordinatesToXcontent(builder, true);
        for(BaseLineStringBuilder<?> hole : holes) {
            hole.coordinatesToXcontent(builder, true);
        }
        return builder;
    }
    
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIELD_TYPE, TYPE.shapename);
        builder.startArray(FIELD_COORDINATES);
        coordinatesArray(builder, params);
        builder.endArray();
        builder.endObject();
        return builder;
    }
    
    public Geometry buildGeometry(GeometryFactory factory, boolean fixDateline) {
        if(fixDateline) {
            Coordinate[][][] polygons = coordinates();
            return polygons.length == 1
                    ? polygon(factory, polygons[0])
                    : multipolygon(factory, polygons);
        } else {
            return toPolygon(factory);
        }
    }

    public Polygon toPolygon() {
        return toPolygon(FACTORY);
    }

    protected Polygon toPolygon(GeometryFactory factory) {
        final LinearRing shell = linearRing(factory, this.shell.points);
        final LinearRing[] holes = new LinearRing[this.holes.size()];
        Iterator<BaseLineStringBuilder<?>> iterator = this.holes.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            holes[i] = linearRing(factory, iterator.next().points);
        }
        return factory.createPolygon(shell, holes);
    }

    protected static LinearRing linearRing(GeometryFactory factory, ArrayList<Coordinate> coordinates) {
        return factory.createLinearRing(coordinates.toArray(new Coordinate[coordinates.size()]));
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    protected static Polygon polygon(GeometryFactory factory, Coordinate[][] polygon) {
        LinearRing shell = factory.createLinearRing(polygon[0]);
        LinearRing[] holes;
        
        if(polygon.length > 1) {
            holes = new LinearRing[polygon.length-1];
            for (int i = 0; i < holes.length; i++) {
                holes[i] = factory.createLinearRing(polygon[i+1]);
            }
        } else {
            holes = null;
        }
        return factory.createPolygon(shell, holes);
    }

    /**
     * Create a Multipolygon from a set of coordinates. Each primary array contains a polygon which
     * in turn contains an array of linestrings. These line Strings are represented as an array of
     * coordinates. The first linestring will be the shell of the polygon the others define holes
     * within the polygon.
     *      
     * @param factory {@link GeometryFactory} to use
     * @param polygons definition of polygons
     * @return a new Multipolygon
     */
    protected static MultiPolygon multipolygon(GeometryFactory factory, Coordinate[][][] polygons) {
        Polygon[] polygonSet = new Polygon[polygons.length];
        for (int i = 0; i < polygonSet.length; i++) {
            polygonSet[i] = polygon(factory, polygons[i]);
        }
        return factory.createMultiPolygon(polygonSet);
    }

    /**
     * This method sets the component id of all edges in a ring to a given id and shifts the
     * coordinates of this component according to the dateline 
     * 
     * @param edge An arbitrary edge of the component
     * @param id id to apply to the component
     * @param edges a list of edges to which all edges of the component will be added (could be <code>null</code>)
     * @return number of edges that belong to this component
     */
    private static int component(final Edge edge, final int id, final ArrayList<Edge> edges) {
        // find a coordinate that is not part of the dateline 
        Edge any = edge;
        while(any.coordinate.x == +DATELINE || any.coordinate.x == -DATELINE) {
            if((any = any.next) == edge) {
                break;   
            }
        }

        double shiftOffset = any.coordinate.x > DATELINE ? DATELINE : (any.coordinate.x < -DATELINE ? -DATELINE : 0);
        if (debugEnabled()) {
            LOGGER.debug("shift: {[]}", shiftOffset);
        }

        // run along the border of the component, collect the
        // edges, shift them according to the dateline and
        // update the component id
        int length = 0, connectedComponents = 0;
        // if there are two connected components, splitIndex keeps track of where to split the edge array
        // start at 1 since the source coordinate is shared
        int splitIndex = 1;
        Edge current = edge;
        Edge prev = edge;
        // bookkeep the source and sink of each visited coordinate
        HashMap<Coordinate, Pair<Edge, Edge>> visitedEdge = new HashMap<>();
        do {
            current.coordinate = shift(current.coordinate, shiftOffset);
            current.component = id;

            if (edges != null) {
                // found a closed loop - we have two connected components so we need to slice into two distinct components
                if (visitedEdge.containsKey(current.coordinate)) {
                    if (connectedComponents > 0 && current.next != edge) {
                        throw new InvalidShapeException("Shape contains more than one shared point");
                    }

                    // a negative id flags the edge as visited for the edges(...) method.
                    // since we're splitting connected components, we want the edges method to visit
                    // the newly separated component
                    final int visitID = -id;
                    Edge firstAppearance = visitedEdge.get(current.coordinate).getRight();
                    // correct the graph pointers by correcting the 'next' pointer for both the
                    // first appearance and this appearance of the edge
                    Edge temp = firstAppearance.next;
                    firstAppearance.next = current.next;
                    current.next = temp;
                    current.component = visitID;
                    // backtrack until we get back to this coordinate, setting the visit id to
                    // a non-visited value (anything positive)
                    do {
                        prev.component = visitID;
                        prev = visitedEdge.get(prev.coordinate).getLeft();
                        ++splitIndex;
                    } while (!current.coordinate.equals(prev.coordinate));
                    ++connectedComponents;
                } else {
                    visitedEdge.put(current.coordinate, Pair.of(prev, current));
                }
                edges.add(current);
                prev = current;
            }
            length++;
        } while(connectedComponents == 0 && (current = current.next) != edge);

        return (splitIndex != 1) ? length-splitIndex: length;
    }

    /**
     * Compute all coordinates of a component
     * @param component an arbitrary edge of the component
     * @param coordinates Array of coordinates to write the result to
     * @return the coordinates parameter
     */
    private static Coordinate[] coordinates(Edge component, Coordinate[] coordinates) {
        for (int i = 0; i < coordinates.length; i++) {
            coordinates[i] = (component = component.next).coordinate;
        }
        return coordinates;
    }

    private static Coordinate[][][] buildCoordinates(ArrayList<ArrayList<Coordinate[]>> components) {
        Coordinate[][][] result = new Coordinate[components.size()][][];
        for (int i = 0; i < result.length; i++) {
            ArrayList<Coordinate[]> component = components.get(i);
            result[i] = component.toArray(new Coordinate[component.size()][]);
        }

        if(debugEnabled()) {
            for (int i = 0; i < result.length; i++) {
                LOGGER.debug("Component {[]}:", i);
                for (int j = 0; j < result[i].length; j++) {
                    LOGGER.debug("\t" + Arrays.toString(result[i][j]));
                }
            }
        }

        return result;
    } 

    private static final Coordinate[][] EMPTY = new Coordinate[0][];

    private static Coordinate[][] holes(Edge[] holes, int numHoles) {
        if (numHoles == 0) {
            return EMPTY;
        }
        final Coordinate[][] points = new Coordinate[numHoles][];

        for (int i = 0; i < numHoles; i++) {
            int length = component(holes[i], -(i+1), null); // mark as visited by inverting the sign
            points[i] = coordinates(holes[i], new Coordinate[length+1]);
        }

        return points;
    } 

    private static Edge[] edges(Edge[] edges, int numHoles, ArrayList<ArrayList<Coordinate[]>> components) {
        ArrayList<Edge> mainEdges = new ArrayList<>(edges.length);

        for (int i = 0; i < edges.length; i++) {
            if (edges[i].component >= 0) {
                int length = component(edges[i], -(components.size()+numHoles+1), mainEdges);
                ArrayList<Coordinate[]> component = new ArrayList<>();
                component.add(coordinates(edges[i], new Coordinate[length+1]));
                components.add(component);
            }
        }

        return mainEdges.toArray(new Edge[mainEdges.size()]);
    }

    private static Coordinate[][][] compose(Edge[] edges, Edge[] holes, int numHoles) {
        final ArrayList<ArrayList<Coordinate[]>> components = new ArrayList<>();
        assign(holes, holes(holes, numHoles), numHoles, edges(edges, numHoles, components), components);
        return buildCoordinates(components);
    }

    private static void assign(Edge[] holes, Coordinate[][] points, int numHoles, Edge[] edges, ArrayList<ArrayList<Coordinate[]>> components) {
        // Assign Hole to related components
        // To find the new component the hole belongs to all intersections of the
        // polygon edges with a vertical line are calculated. This vertical line
        // is an arbitrary point of the hole. The polygon edge next to this point
        // is part of the polygon the hole belongs to.
        if (debugEnabled()) {
            LOGGER.debug("Holes: " + Arrays.toString(holes));
        }
        for (int i = 0; i < numHoles; i++) {
            final Edge current = new Edge(holes[i].coordinate, holes[i].next);
            // the edge intersects with itself at its own coordinate.  We need intersect to be set this way so the binary search 
            // will get the correct position in the edge list and therefore the correct component to add the hole
            current.intersect = current.coordinate;
            final int intersections = intersections(current.coordinate.x, edges);
            // if no intersection is found then the hole is not within the polygon, so
            // don't waste time calling a binary search
            final int pos;
            boolean sharedVertex = false;
            if (intersections == 0 || ((pos = Arrays.binarySearch(edges, 0, intersections, current, INTERSECTION_ORDER)) >= 0)
                            && !(sharedVertex = (edges[pos].intersect.compareTo(current.coordinate) == 0)) ) {
                throw new InvalidShapeException("Invalid shape: Hole is not within polygon");
            }
            final int index = -((sharedVertex) ? 0 : pos+2);
            final int component = -edges[index].component - numHoles - 1;

            if(debugEnabled()) {
                LOGGER.debug("\tposition ("+index+") of edge "+current+": " + edges[index]);
                LOGGER.debug("\tComponent: " + component);
                LOGGER.debug("\tHole intersections ("+current.coordinate.x+"): " + Arrays.toString(edges));
            }

            components.get(component).add(points[i]);
        }
    }

    private static int merge(Edge[] intersections, int offset, int length, Edge[] holes, int numHoles) {
        // Intersections appear pairwise. On the first edge the inner of
        // of the polygon is entered. On the second edge the outer face
        // is entered. Other kinds of intersections are discard by the
        // intersection function

        for (int i = 0; i < length; i += 2) {
            Edge e1 = intersections[offset + i + 0];
            Edge e2 = intersections[offset + i + 1];

            // If two segments are connected maybe a hole must be deleted
            // Since Edges of components appear pairwise we need to check
            // the second edge only (the first edge is either polygon or
            // already handled)
            if (e2.component > 0) {
                //TODO: Check if we could save the set null step
                numHoles--;
                holes[e2.component-1] = holes[numHoles];
                holes[numHoles] = null;
            }
            // only connect edges if intersections are pairwise 
            // 1. per the comment above, the edge array is sorted by y-value of the intersection
            // with the dateline.  Two edges have the same y intercept when they cross the 
            // dateline thus they appear sequentially (pairwise) in the edge array. Two edges
            // do not have the same y intercept when we're forming a multi-poly from a poly
            // that wraps the dateline (but there are 2 ordered intercepts).  
            // The connect method creates a new edge for these paired edges in the linked list. 
            // For boundary conditions (e.g., intersect but not crossing) there is no sibling edge 
            // to connect. Thus the first logic check enforces the pairwise rule
            // 2. the second logic check ensures the two candidate edges aren't already connected by an
            //    existing edge along the dateline - this is necessary due to a logic change in
            //    ShapeBuilder.intersection that computes dateline edges as valid intersect points 
            //    in support of OGC standards
            if (e1.intersect != Edge.MAX_COORDINATE && e2.intersect != Edge.MAX_COORDINATE 
                    && !(e1.next.next.coordinate.equals3D(e2.coordinate) && Math.abs(e1.next.coordinate.x) == DATELINE
                    && Math.abs(e2.coordinate.x) == DATELINE) ) {
                connect(e1, e2);
            }
        }
        return numHoles;
    }

    private static void connect(Edge in, Edge out) {
        assert in != null && out != null;
        assert in != out;
        // Connecting two Edges by inserting the point at
        // dateline intersection and connect these by adding
        // two edges between this points. One per direction
        if(in.intersect != in.next.coordinate) {
            // NOTE: the order of the object creation is crucial here! Don't change it!
            // first edge has no point on dateline
            Edge e1 = new Edge(in.intersect, in.next);
            
            if(out.intersect != out.next.coordinate) {
                // second edge has no point on dateline
                Edge e2 = new Edge(out.intersect, out.next);
                in.next = new Edge(in.intersect, e2, in.intersect);
            } else {
                // second edge intersects with dateline
                in.next = new Edge(in.intersect, out.next, in.intersect);
            }
            out.next = new Edge(out.intersect, e1, out.intersect);
        } else if (in.next != out && in.coordinate != out.intersect) {
            // first edge intersects with dateline
            Edge e2 = new Edge(out.intersect, in.next, out.intersect);

            if(out.intersect != out.next.coordinate) {
                // second edge has no point on dateline
                Edge e1 = new Edge(out.intersect, out.next);
                in.next = new Edge(in.intersect, e1, in.intersect);
                
            } else {
                // second edge intersects with dateline
                in.next = new Edge(in.intersect, out.next, in.intersect);
            }
            out.next = e2;
        }
    }

    private static int createEdges(int component, Orientation orientation, BaseLineStringBuilder<?> shell,
                                   BaseLineStringBuilder<?> hole,
                                   Edge[] edges, int offset) {
        // inner rings (holes) have an opposite direction than the outer rings
        // XOR will invert the orientation for outer ring cases (Truth Table:, T/T = F, T/F = T, F/T = T, F/F = F)
        boolean direction = (component == 0 ^ orientation == Orientation.RIGHT);
        // set the points array accordingly (shell or hole)
        Coordinate[] points = (hole != null) ? hole.coordinates(false) : shell.coordinates(false);
        Edge.ring(component, direction, orientation == Orientation.LEFT, shell, points, 0, edges, offset, points.length-1);
        return points.length-1;
    }

    public static class Ring<P extends ShapeBuilder> extends BaseLineStringBuilder<Ring<P>> {

        private final P parent;

        protected Ring(P parent) {
            this(parent, new ArrayList<Coordinate>());
        }

        protected Ring(P parent, ArrayList<Coordinate> points) {
            super(points);
            this.parent = parent;
        }

        public P close() {
            Coordinate start = points.get(0);
            Coordinate end = points.get(points.size()-1);
            if(start.x != end.x || start.y != end.y) {
                points.add(start);
            }
            return parent;
        }

        @Override
        public GeoShapeType type() {
            return null;
        }
    }
}
