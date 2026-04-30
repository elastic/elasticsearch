/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.SpatialEnvelopeVisitor;

import java.util.List;
import java.util.Locale;
import java.util.Optional;

/**
 * Renders one or more Elasticsearch {@link Geometry} objects to a stand-alone SVG fragment
 * suitable for embedding in function documentation. The rendering is intentionally simple —
 * polygons and lines become {@code path} elements, points become small circles. The combined
 * envelope of all layers determines the viewport.
 *
 * <p>Coordinates are mapped to SVG pixels with the y-axis flipped (since SVG is top-down while
 * geographic coordinates are bottom-up).</p>
 */
public final class GeometryDocSvg {

    /**
     * One geometry to draw, with style.
     *
     * @param geometry    the geometry to render
     * @param fill        SVG fill color, e.g. {@code "#80d0d0"}; {@code "none"} for outline only
     * @param stroke      SVG stroke color
     * @param strokeWidth pixel stroke width
     * @param fillOpacity 0.0 to 1.0
     */
    public record Layer(Geometry geometry, String fill, String stroke, double strokeWidth, double fillOpacity) {

        public static Layer outline(Geometry geometry) {
            return new Layer(geometry, "none", "#888888", 1.5, 1.0);
        }

        public static Layer filled(Geometry geometry) {
            return new Layer(geometry, "#80d0d0", "#0a8aa6", 1.5, 0.6);
        }
    }

    private GeometryDocSvg() {}

    /**
     * Render the given layers to an SVG string. The viewport is sized to {@code width x height}
     * with a small margin around the combined bounding box of the layers' geometries. Aspect
     * ratio of the input is preserved; the unused dimension is whitespace.
     */
    public static String render(int width, int height, List<Layer> layers) {
        Bounds bounds = computeBounds(layers);

        double margin = 0.08;  // 8% margin around the combined envelope
        double marginX = bounds.width() * margin;
        double marginY = bounds.height() * margin;
        double minX = bounds.minX - marginX;
        double maxX = bounds.maxX + marginX;
        double minY = bounds.minY - marginY;
        double maxY = bounds.maxY + marginY;

        double rangeX = maxX - minX;
        double rangeY = maxY - minY;
        double scale = Math.min(width / rangeX, height / rangeY);
        // Center within the viewport when aspect ratios differ.
        double offsetX = (width - rangeX * scale) / 2.0;
        double offsetY = (height - rangeY * scale) / 2.0;

        Mapping map = new Mapping(minX, minY, scale, offsetX, offsetY, height);

        StringBuilder svg = new StringBuilder(2048);
        svg.append(
            String.format(
                Locale.ROOT,
                "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"%d\" height=\"%d\" viewBox=\"0 0 %d %d\">%n",
                width,
                height,
                width,
                height
            )
        );
        for (Layer layer : layers) {
            layer.geometry().visit(new SvgRenderer(svg, layer, map));
        }
        svg.append("</svg>").append(System.lineSeparator());
        return svg.toString();
    }

    /**
     * Combined bounding box of every layer's geometry.
     */
    private static Bounds computeBounds(List<Layer> layers) {
        double minX = Double.POSITIVE_INFINITY;
        double maxX = Double.NEGATIVE_INFINITY;
        double minY = Double.POSITIVE_INFINITY;
        double maxY = Double.NEGATIVE_INFINITY;
        for (Layer layer : layers) {
            Optional<Rectangle> envelope = SpatialEnvelopeVisitor.visitCartesian(layer.geometry());
            if (envelope.isEmpty()) {
                continue;
            }
            Rectangle r = envelope.get();
            minX = Math.min(minX, r.getMinX());
            maxX = Math.max(maxX, r.getMaxX());
            minY = Math.min(minY, r.getMinY());
            maxY = Math.max(maxY, r.getMaxY());
        }
        if (Double.isInfinite(minX)) {
            // No non-empty geometries; pick a unit window so the SVG is still well-formed.
            minX = -1;
            maxX = 1;
            minY = -1;
            maxY = 1;
        }
        // Guard against zero-area envelopes (a single point, or a vertical/horizontal line).
        double rangeX = maxX - minX;
        double rangeY = maxY - minY;
        if (rangeX == 0 && rangeY == 0) {
            minX -= 1;
            maxX += 1;
            minY -= 1;
            maxY += 1;
        } else if (rangeX == 0) {
            double pad = rangeY / 2.0;
            minX -= pad;
            maxX += pad;
        } else if (rangeY == 0) {
            double pad = rangeX / 2.0;
            minY -= pad;
            maxY += pad;
        }
        return new Bounds(minX, maxX, minY, maxY);
    }

    private record Bounds(double minX, double maxX, double minY, double maxY) {
        double width() {
            return maxX - minX;
        }

        double height() {
            return maxY - minY;
        }
    }

    private record Mapping(double minX, double minY, double scale, double offsetX, double offsetY, int height) {
        double x(double lon) {
            return offsetX + (lon - minX) * scale;
        }

        double y(double lat) {
            // Flip Y so larger latitudes are higher on the SVG canvas.
            return height - (offsetY + (lat - minY) * scale);
        }
    }

    /**
     * Renders one geometry into the shared {@link StringBuilder}. Multi-geometries and collections
     * recurse into their elements.
     */
    private record SvgRenderer(StringBuilder svg, Layer layer, Mapping map) implements GeometryVisitor<Void, RuntimeException> {

        @Override
        public Void visit(Point point) {
            if (point.isEmpty()) {
                return null;
            }
            svg.append(
                String.format(
                    Locale.ROOT,
                    "  <circle cx=\"%.2f\" cy=\"%.2f\" r=\"4\" fill=\"%s\" fill-opacity=\"%.2f\" "
                        + "stroke=\"%s\" stroke-width=\"%.2f\"/>%n",
                    map.x(point.getX()),
                    map.y(point.getY()),
                    layer.fill(),
                    layer.fillOpacity(),
                    layer.stroke(),
                    layer.strokeWidth()
                )
            );
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            for (Point p : multiPoint) {
                visit(p);
            }
            return null;
        }

        @Override
        public Void visit(Line line) {
            if (line.isEmpty()) {
                return null;
            }
            StringBuilder d = new StringBuilder();
            appendLine(d, line, false);
            appendPath(d.toString());
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            if (ring.isEmpty()) {
                return null;
            }
            StringBuilder d = new StringBuilder();
            appendLine(d, ring, true);
            appendPath(d.toString());
            return null;
        }

        @Override
        public Void visit(MultiLine multiLine) {
            for (Line line : multiLine) {
                visit(line);
            }
            return null;
        }

        @Override
        public Void visit(Polygon polygon) {
            if (polygon.isEmpty()) {
                return null;
            }
            StringBuilder d = new StringBuilder();
            appendLine(d, polygon.getPolygon(), true);
            for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                d.append(' ');
                appendLine(d, polygon.getHole(i), true);
            }
            appendPath(d.toString());
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            for (Polygon p : multiPolygon) {
                visit(p);
            }
            return null;
        }

        @Override
        public Void visit(Rectangle rectangle) {
            if (rectangle.isEmpty()) {
                return null;
            }
            double minX = rectangle.getMinX();
            double maxX = rectangle.getMaxX();
            double minY = rectangle.getMinY();
            double maxY = rectangle.getMaxY();
            StringBuilder d = new StringBuilder();
            d.append("M ").append(coord(minX, minY));
            d.append(" L ").append(coord(maxX, minY));
            d.append(" L ").append(coord(maxX, maxY));
            d.append(" L ").append(coord(minX, maxY));
            d.append(" Z");
            appendPath(d.toString());
            return null;
        }

        @Override
        public Void visit(Circle circle) {
            // Circle isn't a closed shape geographically, but we can approximate as an SVG circle.
            // (StBuffer/StSimplify never produce Circle, this is just for completeness.)
            svg.append(
                String.format(
                    Locale.ROOT,
                    "  <circle cx=\"%.2f\" cy=\"%.2f\" r=\"%.2f\" fill=\"%s\" fill-opacity=\"%.2f\" "
                        + "stroke=\"%s\" stroke-width=\"%.2f\"/>%n",
                    map.x(circle.getX()),
                    map.y(circle.getY()),
                    circle.getRadiusMeters() * map.scale(),
                    layer.fill(),
                    layer.fillOpacity(),
                    layer.stroke(),
                    layer.strokeWidth()
                )
            );
            return null;
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            for (Geometry g : collection) {
                g.visit(this);
            }
            return null;
        }

        private void appendLine(StringBuilder d, Line line, boolean close) {
            for (int i = 0; i < line.length(); i++) {
                d.append(i == 0 ? "M " : " L ");
                d.append(coord(line.getX(i), line.getY(i)));
            }
            if (close) {
                d.append(" Z");
            }
        }

        private String coord(double x, double y) {
            return String.format(Locale.ROOT, "%.2f %.2f", map.x(x), map.y(y));
        }

        private void appendPath(String path) {
            svg.append(
                String.format(
                    Locale.ROOT,
                    "  <path d=\"%s\" fill=\"%s\" fill-opacity=\"%.2f\" stroke=\"%s\" stroke-width=\"%.2f\" "
                        + "stroke-linejoin=\"round\" stroke-linecap=\"round\" fill-rule=\"evenodd\"/>%n",
                    path,
                    layer.fill(),
                    layer.fillOpacity(),
                    layer.stroke(),
                    layer.strokeWidth()
                )
            );
        }
    }
}
