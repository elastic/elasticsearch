/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.legacygeo.builders;

import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.legacygeo.GeoShapeType;
import org.elasticsearch.legacygeo.parsers.ShapeParser;
import org.elasticsearch.xcontent.XContentBuilder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The {@link PolygonBuilder} implements the groundwork to create polygons. This contains
 * Methods to wrap polygons at the dateline and building shapes from the data held by the
 * builder.
 */
@SuppressWarnings("HiddenField")
public class PolygonBuilder extends ShapeBuilder<JtsGeometry, org.elasticsearch.geometry.Geometry, PolygonBuilder> {

    public static final GeoShapeType TYPE = GeoShapeType.POLYGON;

    private static final Coordinate[][] EMPTY = new Coordinate[0][];

    private Orientation orientation = Orientation.RIGHT;

    // line string defining the shell of the polygon
    private LineStringBuilder shell;

    // List of line strings defining the holes of the polygon
    private final List<LineStringBuilder> holes = new ArrayList<>();

    public PolygonBuilder(LineStringBuilder lineString, Orientation orientation, boolean coerce) {
        this.orientation = orientation;
        if (coerce) {
            lineString.close();
        }
        validateLinearRing(lineString);
        this.shell = lineString;
    }

    public PolygonBuilder(LineStringBuilder lineString, Orientation orientation) {
        this(lineString, orientation, false);
    }

    public PolygonBuilder(CoordinatesBuilder coordinates, Orientation orientation) {
        this(new LineStringBuilder(coordinates), orientation, false);
    }

    public PolygonBuilder(CoordinatesBuilder coordinates) {
        this(coordinates, Orientation.RIGHT);
    }

    /**
     * Read from a stream.
     */
    public PolygonBuilder(StreamInput in) throws IOException {
        shell = new LineStringBuilder(in);
        orientation = Orientation.readFrom(in);
        int holesValue = in.readVInt();
        for (int i = 0; i < holesValue; i++) {
            hole(new LineStringBuilder(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        shell.writeTo(out);
        orientation.writeTo(out);
        out.writeList(holes);
    }

    public Orientation orientation() {
        return this.orientation;
    }

    /**
     * Add a new hole to the polygon
     * @param hole linear ring defining the hole
     * @return this
     */
    public PolygonBuilder hole(LineStringBuilder hole) {
        return this.hole(hole, false);
    }

    /**
     * Add a new hole to the polygon
     * @param hole linear ring defining the hole
     * @param coerce if set to true, it will try to close the hole by adding starting point as end point
     * @return this
     */
    public PolygonBuilder hole(LineStringBuilder hole, boolean coerce) {
        if (coerce) {
            hole.close();
        }
        validateLinearRing(hole);
        holes.add(hole);
        return this;
    }

    /**
     * @return the list of holes defined for this polygon
     */
    public List<LineStringBuilder> holes() {
        return this.holes;
    }

    /**
     * @return the list of points of the shell for this polygon
     */
    public LineStringBuilder shell() {
        return this.shell;
    }

    /**
     * Close the shell of the polygon
     */
    public PolygonBuilder close() {
        shell.close();
        return this;
    }

    private static void validateLinearRing(LineStringBuilder lineString) {
        /**
         * Per GeoJSON spec (http://geojson.org/geojson-spec.html#linestring)
         * A LinearRing is closed LineString with 4 or more positions. The first and last positions
         * are equivalent (they represent equivalent points). Though a LinearRing is not explicitly
         * represented as a GeoJSON geometry type, it is referred to in the Polygon geometry type definition.
         */
        List<Coordinate> points = lineString.coordinates;
        if (points.size() < 4) {
            throw new IllegalArgumentException("invalid number of points in LinearRing (found [" + points.size() + "] - must be >= 4)");
        }

        if (points.get(0).equals(points.get(points.size() - 1)) == false) {
            throw new IllegalArgumentException("invalid LinearRing found (coordinates are not closed)");
        }
    }

    /**
     * Validates only 1 vertex is tangential (shared) between the interior and exterior of a polygon
     */
    protected static void validateHole(LineStringBuilder shell, LineStringBuilder hole) {
        HashSet<Coordinate> exterior = Sets.newHashSet(shell.coordinates);
        HashSet<Coordinate> interior = Sets.newHashSet(hole.coordinates);
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
        LineStringBuilder shell = filterRing(this.shell);
        LineStringBuilder[] holes = new LineStringBuilder[this.holes.size()];
        int numEdges = shell.coordinates.size() - 1; // Last point is repeated
        for (int i = 0; i < this.holes.size(); i++) {
            holes[i] = filterRing(this.holes.get(i));
            numEdges += holes[i].coordinates.size() - 1;
            validateHole(shell, holes[i]);
        }

        Edge[] edges = new Edge[numEdges];
        Edge[] holeComponents = new Edge[holes.length];
        final AtomicBoolean translated = new AtomicBoolean(false);
        int offset = createEdges(0, orientation, shell, null, edges, 0, translated);
        for (int i = 0; i < holes.length; i++) {
            int length = createEdges(i + 1, orientation, shell, holes[i], edges, offset, translated);
            holeComponents[i] = edges[offset];
            offset += length;
        }

        int numHoles = holeComponents.length;

        numHoles = merge(edges, 0, intersections(+DATELINE, edges), holeComponents, numHoles);
        numHoles = merge(edges, 0, intersections(-DATELINE, edges), holeComponents, numHoles);

        return compose(edges, holeComponents, numHoles);
    }

    /**
     * This method removes duplicated points and coplanar points on vertical lines (vertical lines
     * do not cross the dateline).
     */
    private static LineStringBuilder filterRing(LineStringBuilder linearRing) {
        int numPoints = linearRing.coordinates.size();
        List<Coordinate> coordinates = new ArrayList<>();
        coordinates.add(linearRing.coordinates.get(0));
        for (int i = 1; i < numPoints - 1; i++) {
            if (linearRing.coordinates.get(i - 1).x == linearRing.coordinates.get(i).x) {
                if (linearRing.coordinates.get(i - 1).y == linearRing.coordinates.get(i).y) {
                    // same point
                    continue;
                }
                if (linearRing.coordinates.get(i - 1).x == linearRing.coordinates.get(i + 1).x
                    && linearRing.coordinates.get(i - 1).y > linearRing.coordinates.get(i).y != linearRing.coordinates.get(
                        i + 1
                    ).y > linearRing.coordinates.get(i).y) {
                    // coplanar
                    continue;
                }
            }
            coordinates.add(linearRing.coordinates.get(i));
        }
        coordinates.add(linearRing.coordinates.get(numPoints - 1));
        return new LineStringBuilder(coordinates);
    }

    @Override
    public JtsGeometry buildS4J() {
        return jtsGeometry(buildS4JGeometry(FACTORY, wrapdateline));
    }

    @Override
    public org.elasticsearch.geometry.Geometry buildGeometry() {
        return toPolygonGeometry();
    }

    protected XContentBuilder coordinatesArray(XContentBuilder builder, Params params) throws IOException {
        shell.coordinatesToXcontent(builder, true);
        for (LineStringBuilder hole : holes) {
            hole.coordinatesToXcontent(builder, true);
        }
        return builder;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ShapeParser.FIELD_TYPE.getPreferredName(), TYPE.shapeName());
        builder.field(ShapeParser.FIELD_ORIENTATION.getPreferredName(), orientation.name().toLowerCase(Locale.ROOT));
        builder.startArray(ShapeParser.FIELD_COORDINATES.getPreferredName());
        coordinatesArray(builder, params);
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public Geometry buildS4JGeometry(GeometryFactory factory, boolean fixDateline) {
        if (fixDateline) {
            Coordinate[][][] polygons = coordinates();
            return polygons.length == 1 ? polygonS4J(factory, polygons[0]) : multipolygonS4J(factory, polygons);
        } else {
            return toPolygonS4J(factory);
        }
    }

    public Polygon toPolygonS4J() {
        return toPolygonS4J(FACTORY);
    }

    protected Polygon toPolygonS4J(GeometryFactory factory) {
        final LinearRing shell = linearRingS4J(factory, this.shell.coordinates);
        final LinearRing[] holes = new LinearRing[this.holes.size()];
        Iterator<LineStringBuilder> iterator = this.holes.iterator();
        for (int i = 0; iterator.hasNext(); i++) {
            holes[i] = linearRingS4J(factory, iterator.next().coordinates);
        }
        return factory.createPolygon(shell, holes);
    }

    public org.elasticsearch.geometry.Polygon toPolygonGeometry() {
        final List<org.elasticsearch.geometry.LinearRing> holes = new ArrayList<>(this.holes.size());
        for (int i = 0; i < this.holes.size(); ++i) {
            holes.add(linearRing(this.holes.get(i).coordinates));
        }
        return new org.elasticsearch.geometry.Polygon(linearRing(this.shell.coordinates), holes);
    }

    protected static org.elasticsearch.geometry.LinearRing linearRing(List<Coordinate> coordinates) {
        return new org.elasticsearch.geometry.LinearRing(
            coordinates.stream().mapToDouble(i -> i.x).toArray(),
            coordinates.stream().mapToDouble(i -> i.y).toArray()
        );
    }

    protected static LinearRing linearRingS4J(GeometryFactory factory, List<Coordinate> coordinates) {
        return factory.createLinearRing(coordinates.toArray(new Coordinate[coordinates.size()]));
    }

    @Override
    public GeoShapeType type() {
        return TYPE;
    }

    @Override
    public int numDimensions() {
        if (shell == null) {
            throw new IllegalStateException("unable to get number of dimensions, " + "Polygon has not yet been initialized");
        }
        return shell.numDimensions();
    }

    protected static Polygon polygonS4J(GeometryFactory factory, Coordinate[][] polygon) {
        LinearRing shell = factory.createLinearRing(polygon[0]);
        LinearRing[] holes;

        if (polygon.length > 1) {
            holes = new LinearRing[polygon.length - 1];
            for (int i = 0; i < holes.length; i++) {
                holes[i] = factory.createLinearRing(polygon[i + 1]);
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
    protected static MultiPolygon multipolygonS4J(GeometryFactory factory, Coordinate[][][] polygons) {
        Polygon[] polygonSet = new Polygon[polygons.length];
        for (int i = 0; i < polygonSet.length; i++) {
            polygonSet[i] = polygonS4J(factory, polygons[i]);
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
    private static int component(final Edge edge, final int id, final ArrayList<Edge> edges, double[] partitionPoint) {
        // find a coordinate that is not part of the dateline
        Edge any = edge;
        while (any.coordinate.x == +DATELINE || any.coordinate.x == -DATELINE) {
            if ((any = any.next) == edge) {
                break;
            }
        }

        double shiftOffset = any.coordinate.x > DATELINE ? DATELINE : (any.coordinate.x < -DATELINE ? -DATELINE : 0);
        if (debugEnabled()) {
            LOGGER.debug("shift: [{}]", shiftOffset);
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
        HashMap<Coordinate, Tuple<Edge, Edge>> visitedEdge = new HashMap<>();
        do {
            current.coordinate = shift(current.coordinate, shiftOffset);
            current.component = id;

            if (edges != null) {
                // found a closed loop - we have two connected components so we need to slice into two distinct components
                if (visitedEdge.containsKey(current.coordinate)) {
                    partitionPoint[0] = current.coordinate.x;
                    partitionPoint[1] = current.coordinate.y;
                    partitionPoint[2] = current.coordinate.z;
                    if (connectedComponents > 0 && current.next != edge) {
                        throw new InvalidShapeException("Shape contains more than one shared point");
                    }

                    // a negative id flags the edge as visited for the edges(...) method.
                    // since we're splitting connected components, we want the edges method to visit
                    // the newly separated component
                    final int visitID = -id;
                    Edge firstAppearance = visitedEdge.get(current.coordinate).v2();
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
                        prev = visitedEdge.get(prev.coordinate).v1();
                        ++splitIndex;
                    } while (current.coordinate.equals(prev.coordinate) == false);
                    ++connectedComponents;
                } else {
                    visitedEdge.put(current.coordinate, new Tuple<Edge, Edge>(prev, current));
                }
                edges.add(current);
                prev = current;
            }
            length++;
        } while (connectedComponents == 0 && (current = current.next) != edge);

        return (splitIndex != 1) ? length - splitIndex : length;
    }

    /**
     * Compute all coordinates of a component
     * @param component an arbitrary edge of the component
     * @param coordinates Array of coordinates to write the result to
     * @return the coordinates parameter
     */
    private static Coordinate[] coordinates(Edge component, Coordinate[] coordinates, double[] partitionPoint) {
        for (int i = 0; i < coordinates.length; i++) {
            coordinates[i] = (component = component.next).coordinate;
        }
        // First and last coordinates must be equal
        if (coordinates[0].equals(coordinates[coordinates.length - 1]) == false) {
            if (Double.isNaN(partitionPoint[2])) {
                throw new InvalidShapeException("Self-intersection at or near point [" + partitionPoint[0] + "," + partitionPoint[1] + "]");
            } else {
                throw new InvalidShapeException(
                    "Self-intersection at or near point [" + partitionPoint[0] + "," + partitionPoint[1] + "," + partitionPoint[2] + "]"
                );
            }
        }
        return coordinates;
    }

    private static Coordinate[][][] buildCoordinates(List<List<Coordinate[]>> components) {
        Coordinate[][][] result = new Coordinate[components.size()][][];
        for (int i = 0; i < result.length; i++) {
            List<Coordinate[]> component = components.get(i);
            result[i] = component.toArray(new Coordinate[component.size()][]);
        }

        if (debugEnabled()) {
            for (int i = 0; i < result.length; i++) {
                LOGGER.debug("Component [{}]:", i);
                for (int j = 0; j < result[i].length; j++) {
                    LOGGER.debug("\t{}", Arrays.toString(result[i][j]));
                }
            }
        }

        return result;
    }

    private static Coordinate[][] holes(Edge[] holes, int numHoles) {
        if (numHoles == 0) {
            return EMPTY;
        }
        final Coordinate[][] points = new Coordinate[numHoles][];

        for (int i = 0; i < numHoles; i++) {
            double[] partitionPoint = new double[3];
            int length = component(holes[i], -(i + 1), null, partitionPoint); // mark as visited by inverting the sign
            points[i] = coordinates(holes[i], new Coordinate[length + 1], partitionPoint);
        }

        return points;
    }

    private static Edge[] edges(Edge[] edges, int numHoles, List<List<Coordinate[]>> components) {
        ArrayList<Edge> mainEdges = new ArrayList<>(edges.length);

        for (int i = 0; i < edges.length; i++) {
            if (edges[i].component >= 0) {
                double[] partitionPoint = new double[3];
                int length = component(edges[i], -(components.size() + numHoles + 1), mainEdges, partitionPoint);
                List<Coordinate[]> component = new ArrayList<>();
                component.add(coordinates(edges[i], new Coordinate[length + 1], partitionPoint));
                components.add(component);
            }
        }

        return mainEdges.toArray(new Edge[mainEdges.size()]);
    }

    private static Coordinate[][][] compose(Edge[] edges, Edge[] holes, int numHoles) {
        final List<List<Coordinate[]>> components = new ArrayList<>();
        assign(holes, holes(holes, numHoles), numHoles, edges(edges, numHoles, components), components);
        return buildCoordinates(components);
    }

    private static void assign(Edge[] holes, Coordinate[][] points, int numHoles, Edge[] edges, List<List<Coordinate[]>> components) {
        // Assign Hole to related components
        // To find the new component the hole belongs to all intersections of the
        // polygon edges with a vertical line are calculated. This vertical line
        // is an arbitrary point of the hole. The polygon edge next to this point
        // is part of the polygon the hole belongs to.
        if (debugEnabled()) {
            LOGGER.debug("Holes: {}", Arrays.toString(holes));
        }
        for (int i = 0; i < numHoles; i++) {
            // To do the assignment we assume (and later, elsewhere, check) that each hole is within
            // a single component, and the components do not overlap. Based on this assumption, it's
            // enough to find a component that contains some vertex of the hole, and
            // holes[i].coordinate is such a vertex, so we use that one.

            // First, we sort all the edges according to their order of intersection with the line
            // of longitude through holes[i].coordinate, in order from south to north. Edges that do
            // not intersect this line are sorted to the end of the array and of no further interest
            // here.
            final Edge current = new Edge(holes[i].coordinate, holes[i].next);
            current.intersect = current.coordinate;
            final int intersections = intersections(current.coordinate.x, edges);

            if (intersections == 0) {
                // There were no edges that intersect the line of longitude through
                // holes[i].coordinate, so there's no way this hole is within the polygon.
                throw new InvalidShapeException("Invalid shape: Hole is not within polygon");
            }

            // Next we do a binary search to find the position of holes[i].coordinate in the array.
            // The binary search returns the index of an exact match, or (-insertionPoint - 1) if
            // the vertex lies between the intersections of edges[insertionPoint] and
            // edges[insertionPoint+1]. The latter case is vastly more common.

            final int pos;
            boolean sharedVertex = false;
            if (((pos = Arrays.binarySearch(edges, 0, intersections, current, INTERSECTION_ORDER)) >= 0)
                && (sharedVertex = (edges[pos].intersect.compareTo(current.coordinate) == 0)) == false) {
                // The binary search returned an exact match, but we checked again using compareTo()
                // and it didn't match after all.

                // TODO Can this actually happen? Needs a test to exercise it, or else needs to be removed.
                throw new InvalidShapeException("Invalid shape: Hole is not within polygon");
            }

            final int index;
            if (sharedVertex) {
                // holes[i].coordinate lies exactly on an edge.
                index = 0; // TODO Should this be pos instead of 0? This assigns exact matches to the southernmost component.
            } else if (pos == -1) {
                // holes[i].coordinate is strictly south of all intersections. Assign it to the
                // southernmost component, and allow later validation to spot that it is not
                // entirely within the chosen component.
                index = 0;
            } else {
                // holes[i].coordinate is strictly north of at least one intersection. Assign it to
                // the component immediately to its south.
                index = -(pos + 2);
            }

            final int component = -edges[index].component - numHoles - 1;

            if (debugEnabled()) {
                LOGGER.debug("\tposition ({}) of edge {}: {}", index, current, edges[index]);
                LOGGER.debug("\tComponent: {}", component);
                LOGGER.debug("\tHole intersections ({}): {}", current.coordinate.x, Arrays.toString(edges));
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
                // TODO: Check if we could save the set null step
                numHoles--;
                holes[e2.component - 1] = holes[numHoles];
                holes[numHoles] = null;
            }
            // only connect edges if intersections are pairwise
            // 1. per the comment above, the edge array is sorted by y-value of the intersection
            // with the dateline. Two edges have the same y intercept when they cross the
            // dateline thus they appear sequentially (pairwise) in the edge array. Two edges
            // do not have the same y intercept when we're forming a multi-poly from a poly
            // that wraps the dateline (but there are 2 ordered intercepts).
            // The connect method creates a new edge for these paired edges in the linked list.
            // For boundary conditions (e.g., intersect but not crossing) there is no sibling edge
            // to connect. Thus the first logic check enforces the pairwise rule
            // 2. the second logic check ensures the two candidate edges aren't already connected by an
            // existing edge along the dateline - this is necessary due to a logic change in
            // ShapeBuilder.intersection that computes dateline edges as valid intersect points
            // in support of OGC standards
            if (e1.intersect != Edge.MAX_COORDINATE
                && e2.intersect != Edge.MAX_COORDINATE
                && (e1.next.next.coordinate.equals3D(e2.coordinate)
                    && Math.abs(e1.next.coordinate.x) == DATELINE
                    && Math.abs(e2.coordinate.x) == DATELINE) == false) {
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
        if (in.intersect != in.next.coordinate) {
            // NOTE: the order of the object creation is crucial here! Don't change it!
            // first edge has no point on dateline
            Edge e1 = new Edge(in.intersect, in.next);

            if (out.intersect != out.next.coordinate) {
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

            if (out.intersect != out.next.coordinate) {
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

    private static int createEdges(
        int component,
        Orientation orientation,
        LineStringBuilder shell,
        LineStringBuilder hole,
        Edge[] edges,
        int offset,
        final AtomicBoolean translated
    ) {
        // inner rings (holes) have an opposite direction than the outer rings
        // XOR will invert the orientation for outer ring cases (Truth Table:, T/T = F, T/F = T, F/T = T, F/F = F)
        boolean direction = (component == 0 ^ orientation == Orientation.RIGHT);
        // set the points array accordingly (shell or hole)
        Coordinate[] points = (hole != null) ? hole.coordinates(false) : shell.coordinates(false);
        ring(component, direction, orientation == Orientation.LEFT, points, 0, edges, offset, points.length - 1, translated);
        return points.length - 1;
    }

    /**
     * Create a connected list of a list of coordinates
     *
     * @param points
     *            array of point
     * @param offset
     *            index of the first point
     * @param length
     *            number of points
     * @return Array of edges
     */
    private static Edge[] ring(
        int component,
        boolean direction,
        boolean handedness,
        Coordinate[] points,
        int offset,
        Edge[] edges,
        int toffset,
        int length,
        final AtomicBoolean translated
    ) {
        double signedArea = 0;
        double minX = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        for (int i = offset; i < offset + length; i++) {
            signedArea += points[i].x * points[i + 1].y - points[i].y * points[i + 1].x;
            minX = Math.min(minX, points[i].x);
            maxX = Math.max(maxX, points[i].x);
        }

        // if the polygon is tiny, the computed area can result in zero. In that case
        // we assume orientation is correct
        boolean orientation = signedArea == 0 ? handedness : signedArea < 0;

        // OGC requires shell as ccw (Right-Handedness) and holes as cw (Left-Handedness)
        // since GeoJSON doesn't specify (and doesn't need to) GEO core will assume OGC standards
        // thus if orientation is computed as cw, the logic will translate points across dateline
        // and convert to a right handed system

        // calculate range
        final double rng = maxX - minX;
        // translate the points if the following is true
        // 1. shell orientation is cw and range is greater than a hemisphere (180 degrees) but not spanning 2 hemispheres
        // (translation would result in a collapsed poly)
        // 2. the shell of the candidate hole has been translated (to preserve the coordinate system)
        boolean incorrectOrientation = component == 0 && handedness != orientation;
        if ((incorrectOrientation && (rng > DATELINE && rng != 2 * DATELINE)) || (translated.get() && component != 0)) {
            translate(points);
            // flip the translation bit if the shell is being translated
            if (component == 0) {
                translated.set(true);
            }
            // correct the orientation post translation (ccw for shell, cw for holes)
            if (component == 0 || (component != 0 && handedness == orientation)) {
                orientation = orientation == false;
            }
        }
        return concat(component, direction ^ orientation, points, offset, edges, toffset, length);
    }

    /**
     * Concatenate a set of points to a polygon
     *
     * @param component
     *            component id of the polygon
     * @param direction
     *            direction of the ring
     * @param points
     *            list of points to concatenate
     * @param pointOffset
     *            index of the first point
     * @param edges
     *            Array of edges to write the result to
     * @param edgeOffset
     *            index of the first edge in the result
     * @param length
     *            number of points to use
     * @return the edges creates
     */
    private static Edge[] concat(
        int component,
        boolean direction,
        Coordinate[] points,
        final int pointOffset,
        Edge[] edges,
        final int edgeOffset,
        int length
    ) {
        assert edges.length >= length + edgeOffset;
        assert points.length >= length + pointOffset;
        edges[edgeOffset] = new Edge(points[pointOffset], null);
        for (int i = 1; i < length; i++) {
            if (direction) {
                edges[edgeOffset + i] = new Edge(points[pointOffset + i], edges[edgeOffset + i - 1]);
                edges[edgeOffset + i].component = component;
            } else if (edges[edgeOffset + i - 1].coordinate.equals(points[pointOffset + i]) == false) {
                edges[edgeOffset + i - 1].next = edges[edgeOffset + i] = new Edge(points[pointOffset + i], null);
                edges[edgeOffset + i - 1].component = component;
            } else {
                throw new InvalidShapeException("Provided shape has duplicate consecutive coordinates at: " + points[pointOffset + i]);
            }
        }

        if (direction) {
            edges[edgeOffset].setNext(edges[edgeOffset + length - 1]);
            edges[edgeOffset].component = component;
        } else {
            edges[edgeOffset + length - 1].setNext(edges[edgeOffset]);
            edges[edgeOffset + length - 1].component = component;
        }

        return edges;
    }

    /**
     * Transforms coordinates in the eastern hemisphere (-180:0) to a (180:360) range
     */
    private static void translate(Coordinate[] points) {
        for (Coordinate c : points) {
            if (c.x < 0) {
                c.x += 2 * DATELINE;
            }
        }
    }

    @Override
    protected StringBuilder contentToWKT() {
        StringBuilder sb = new StringBuilder();
        sb.append('(');
        sb.append(ShapeBuilder.coordinateListToWKT(shell.coordinates));
        for (LineStringBuilder hole : holes) {
            sb.append(", ");
            sb.append(ShapeBuilder.coordinateListToWKT(hole.coordinates));
        }
        sb.append(')');
        return sb;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shell, holes, orientation);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        PolygonBuilder other = (PolygonBuilder) obj;
        return Objects.equals(shell, other.shell) && Objects.equals(holes, other.holes) && Objects.equals(orientation, other.orientation);
    }
}
