/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.geo.GeoPolygonDecomposer;
import org.elasticsearch.common.geo.GeoShapeUtils;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.time.DateMathParser;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeQueryable;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.runtimefields.fielddata.GeoPointScriptFieldData;
import org.elasticsearch.xpack.runtimefields.query.GeoPointScriptFieldExistsQuery;
import org.elasticsearch.xpack.runtimefields.query.GeoPointScriptFieldGeoShapeQuery;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public final class GeoPointScriptMappedFieldType extends AbstractScriptFieldType<GeoPointFieldScript.LeafFactory>
    implements
        GeoShapeQueryable {

    GeoPointScriptMappedFieldType(String name, Script script, GeoPointFieldScript.Factory scriptFactory, Map<String, String> meta) {
        super(name, script, scriptFactory::newFactory, meta);
    }

    @Override
    protected String runtimeType() {
        return GeoPointFieldMapper.CONTENT_TYPE;
    }

    @Override
    protected Query rangeQuery(
        Object lowerTerm,
        Object upperTerm,
        boolean includeLower,
        boolean includeUpper,
        ZoneId timeZone,
        DateMathParser parser,
        QueryShardContext context
    ) {
        throw new IllegalArgumentException("Field [" + name() + "] of type [" + typeName() + "] does not support range queries");
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        throw new IllegalArgumentException(
            "Geometry fields do not support exact searching, use dedicated geometry queries instead: [" + name() + "]"
        );
    }

    @Override
    public GeoPointScriptFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
        return new GeoPointScriptFieldData.Builder(name(), leafFactory(searchLookup.get()));
    }

    @Override
    public Query existsQuery(QueryShardContext context) {
        checkAllowExpensiveQueries(context);
        return new GeoPointScriptFieldExistsQuery(script, leafFactory(context), name());
    }

    @Override
    public Query geoShapeQuery(Geometry shape, String fieldName, ShapeRelation relation, QueryShardContext context) {
        if (shape == null) {
            return new MatchNoDocsQuery();
        }
        final LatLonGeometry[] geometries = toLuceneGeometry(fieldName, context, shape);
        return new GeoPointScriptFieldGeoShapeQuery(script, leafFactory(context), fieldName, geometries);
    }

    private static LatLonGeometry[] toLuceneGeometry(String name, QueryShardContext context, Geometry geometry) {
        final List<LatLonGeometry> geometries = new ArrayList<>();
        geometry.visit(new GeometryVisitor<>() {
            @Override
            public Void visit(Circle circle) {
                if (circle.isEmpty() == false) {
                    geometries.add(GeoShapeUtils.toLuceneCircle(circle));
                }
                return null;
            }

            @Override
            public Void visit(GeometryCollection<?> collection) {
                for (Geometry shape : collection) {
                    shape.visit(this);
                }
                return null;
            }

            @Override
            public Void visit(org.elasticsearch.geometry.Line line) {
                throw new QueryShardException(context, "Field [" + name + "] found and unsupported shape Line");
            }

            @Override
            public Void visit(LinearRing ring) {
                throw new QueryShardException(context, "Field [" + name + "] found and unsupported shape LinearRing");
            }

            @Override
            public Void visit(MultiLine multiLine) {
                throw new QueryShardException(context, "Field [" + name + "] found and unsupported shape MultiLine");
            }

            @Override
            public Void visit(MultiPoint multiPoint) {
                for (Point point : multiPoint) {
                    visit(point);
                }
                return null;
            }

            @Override
            public Void visit(MultiPolygon multiPolygon) {
                if (multiPolygon.isEmpty() == false) {
                    List<org.elasticsearch.geometry.Polygon> collector = new ArrayList<>();
                    GeoPolygonDecomposer.decomposeMultiPolygon(multiPolygon, true, collector);
                    collector.forEach((p) -> geometries.add(GeoShapeUtils.toLucenePolygon(p)));
                }
                return null;
            }

            @Override
            public Void visit(Point point) {
                if (point.isEmpty() == false) {
                    geometries.add(GeoShapeUtils.toLucenePoint(point));
                }
                return null;

            }

            @Override
            public Void visit(org.elasticsearch.geometry.Polygon polygon) {
                if (polygon.isEmpty() == false) {
                    List<org.elasticsearch.geometry.Polygon> collector = new ArrayList<>();
                    GeoPolygonDecomposer.decomposePolygon(polygon, true, collector);
                    collector.forEach((p) -> geometries.add(GeoShapeUtils.toLucenePolygon(p)));
                }
                return null;
            }

            @Override
            public Void visit(Rectangle r) {
                if (r.isEmpty() == false) {
                    geometries.add(GeoShapeUtils.toLuceneRectangle(r));
                }
                return null;
            }
        });
        return geometries.toArray(new LatLonGeometry[geometries.size()]);
    }
}
