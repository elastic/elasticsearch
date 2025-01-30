/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.geo.Polygon;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.mapper.GeoPointFieldMapper.GeoPointFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Deprecated geo query. Deprecated in #64227, 7.12/8.0. We do not plan to remove this so we
 * do not break any users using this.
 * @deprecated use {@link GeoShapeQueryBuilder}
 */
@Deprecated
public class GeoPolygonQueryBuilder extends AbstractQueryBuilder<GeoPolygonQueryBuilder> {
    public static final String NAME = "geo_polygon";

    public static final String GEO_POLYGON_DEPRECATION_MSG = "["
        + GeoShapeQueryBuilder.NAME
        + "] query "
        + "where polygons are defined in geojson or wkt";

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;
    private static final ParseField VALIDATION_METHOD = new ParseField("validation_method");
    private static final ParseField POINTS_FIELD = new ParseField("points");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final String fieldName;

    private final List<GeoPoint> shell;

    private GeoValidationMethod validationMethod = GeoValidationMethod.DEFAULT;

    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

    @Deprecated
    public GeoPolygonQueryBuilder(String fieldName, List<GeoPoint> points) {
        if (Strings.isEmpty(fieldName)) {
            throw new IllegalArgumentException("fieldName must not be null");
        }
        if (points == null || points.isEmpty()) {
            throw new IllegalArgumentException("polygon must not be null or empty");
        } else {
            GeoPoint start = points.get(0);
            if (start.equals(points.get(points.size() - 1))) {
                if (points.size() < 4) {
                    throw new IllegalArgumentException("too few points defined for geo_polygon query");
                }
            } else {
                if (points.size() < 3) {
                    throw new IllegalArgumentException("too few points defined for geo_polygon query");
                }
            }
        }
        this.fieldName = fieldName;
        this.shell = new ArrayList<>(points);
        if (shell.get(shell.size() - 1).equals(shell.get(0)) == false) {
            shell.add(shell.get(0));
        }
    }

    /**
     * Read from a stream.
     */
    public GeoPolygonQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        int size = in.readVInt();
        shell = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            shell.add(in.readGeoPoint());
        }
        validationMethod = GeoValidationMethod.readFromStream(in);
        ignoreUnmapped = in.readBoolean();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeCollection(shell, StreamOutput::writeGeoPoint);
        validationMethod.writeTo(out);
        out.writeBoolean(ignoreUnmapped);
    }

    public String fieldName() {
        return fieldName;
    }

    public List<GeoPoint> points() {
        return shell;
    }

    /** Sets the validation method to use for geo coordinates. */
    public GeoPolygonQueryBuilder setValidationMethod(GeoValidationMethod method) {
        this.validationMethod = method;
        return this;
    }

    /**
     * Sets whether the query builder should ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public GeoPolygonQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        return this;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        MappedFieldType fieldType = context.getFieldType(fieldName);
        if (fieldType == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "failed to find geo_point field [" + fieldName + "]");
            }
        }
        if ((fieldType instanceof GeoPointFieldType) == false) {
            throw new QueryShardException(context, "field [" + fieldName + "] is not a geo_point field");
        }

        List<GeoPoint> shell = new ArrayList<>(this.shell.size());
        for (GeoPoint geoPoint : this.shell) {
            shell.add(new GeoPoint(geoPoint));
        }
        final int shellSize = shell.size();

        // validation was not available prior to 2.x, so to support bwc
        // percolation queries we only ignore_malformed on 2.x created indexes
        if (GeoValidationMethod.isIgnoreMalformed(validationMethod) == false) {
            for (GeoPoint point : shell) {
                if (GeoUtils.isValidLatitude(point.lat()) == false) {
                    throw new QueryShardException(
                        context,
                        "illegal latitude value [{}] for [{}]",
                        point.lat(),
                        GeoPolygonQueryBuilder.NAME
                    );
                }
                if (GeoUtils.isValidLongitude(point.lon()) == false) {
                    throw new QueryShardException(
                        context,
                        "illegal longitude value [{}] for [{}]",
                        point.lon(),
                        GeoPolygonQueryBuilder.NAME
                    );
                }
            }
        }

        if (GeoValidationMethod.isCoerce(validationMethod)) {
            for (GeoPoint point : shell) {
                GeoUtils.normalizePoint(point, true, true);
            }
        }

        double[] lats = new double[shellSize];
        double[] lons = new double[shellSize];
        GeoPoint p;
        for (int i = 0; i < shellSize; ++i) {
            p = shell.get(i);
            lats[i] = p.lat();
            lons[i] = p.lon();
        }

        Polygon polygon = new Polygon(lats, lons);
        Query query = LatLonPoint.newPolygonQuery(fieldType.name(), polygon);
        if (fieldType.hasDocValues()) {
            Query dvQuery = LatLonDocValuesField.newSlowPolygonQuery(fieldType.name(), polygon);
            query = new IndexOrDocValuesQuery(query, dvQuery);
        }
        return query;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.startObject(fieldName);
        builder.startArray(POINTS_FIELD.getPreferredName());
        for (GeoPoint point : shell) {
            builder.startArray().value(point.lon()).value(point.lat()).endArray();
        }
        builder.endArray();
        builder.endObject();

        builder.field(VALIDATION_METHOD.getPreferredName(), validationMethod);
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);

        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static GeoPolygonQueryBuilder fromXContent(XContentParser parser) throws IOException {
        String fieldName = null;

        List<GeoPoint> shell = null;

        Float boost = null;
        GeoValidationMethod validationMethod = null;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if (POINTS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            shell = new ArrayList<>();
                            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                                shell.add(GeoUtils.parseGeoPoint(parser));
                            }
                        } else {
                            throw new ParsingException(
                                parser.getTokenLocation(),
                                "[geo_polygon] query does not support [" + currentFieldName + "]"
                            );
                        }
                    } else {
                        throw new ParsingException(
                            parser.getTokenLocation(),
                            "[geo_polygon] query does not support token type [" + token.name() + "] under [" + currentFieldName + "]"
                        );
                    }
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (VALIDATION_METHOD.match(currentFieldName, parser.getDeprecationHandler())) {
                    validationMethod = GeoValidationMethod.fromString(parser.text());
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "[geo_polygon] query does not support [" + currentFieldName + "]"
                    );
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[geo_polygon] unexpected token type [" + token.name() + "]");
            }
        }
        GeoPolygonQueryBuilder builder = new GeoPolygonQueryBuilder(fieldName, shell);
        if (validationMethod != null) {
            // if GeoValidationMethod was explicitly set ignore deprecated coerce and ignoreMalformed settings
            builder.setValidationMethod(validationMethod);
        }

        if (queryName != null) {
            builder.queryName(queryName);
        }
        if (boost != null) {
            builder.boost(boost);
        }
        builder.ignoreUnmapped(ignoreUnmapped);
        return builder;
    }

    @Override
    protected boolean doEquals(GeoPolygonQueryBuilder other) {
        return Objects.equals(validationMethod, other.validationMethod)
            && Objects.equals(fieldName, other.fieldName)
            && Objects.equals(shell, other.shell)
            && Objects.equals(ignoreUnmapped, other.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(validationMethod, fieldName, shell, ignoreUnmapped);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
