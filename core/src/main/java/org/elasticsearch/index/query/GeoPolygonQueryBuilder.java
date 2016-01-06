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

package org.elasticsearch.index.query;

import org.apache.lucene.search.GeoPointInPolygonQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.BaseGeoPointFieldMapper;
import org.elasticsearch.index.search.geo.GeoPolygonQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class GeoPolygonQueryBuilder extends AbstractQueryBuilder<GeoPolygonQueryBuilder> {

    public static final String NAME = "geo_polygon";

    private static final List<GeoPoint> PROTO_SHAPE = Arrays.asList(new GeoPoint[] { new GeoPoint(1.0, 1.0), new GeoPoint(1.0, 2.0),
            new GeoPoint(2.0, 1.0) });

    static final GeoPolygonQueryBuilder PROTOTYPE = new GeoPolygonQueryBuilder("field", PROTO_SHAPE);

    private final String fieldName;

    private final List<GeoPoint> shell;

    private GeoValidationMethod validationMethod = GeoValidationMethod.DEFAULT;

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
        if (!shell.get(shell.size() - 1).equals(shell.get(0))) {
            shell.add(shell.get(0));
        }
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

    /** Returns the validation method to use for geo coordinates. */
    public GeoValidationMethod getValidationMethod() {
        return this.validationMethod;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new QueryShardException(context, "failed to find geo_point field [" + fieldName + "]");
        }
        if (!(fieldType instanceof BaseGeoPointFieldMapper.GeoPointFieldType)) {
            throw new QueryShardException(context, "field [" + fieldName + "] is not a geo_point field");
        }

        List<GeoPoint> shell = new ArrayList<GeoPoint>();
        for (GeoPoint geoPoint : this.shell) {
            shell.add(new GeoPoint(geoPoint));
        }
        final int shellSize = shell.size();

        final boolean indexCreatedBeforeV2_0 = context.indexVersionCreated().before(Version.V_2_0_0);
        // validation was not available prior to 2.x, so to support bwc
        // percolation queries we only ignore_malformed on 2.x created indexes
        if (!indexCreatedBeforeV2_0 && !GeoValidationMethod.isIgnoreMalformed(validationMethod)) {
            for (GeoPoint point : shell) {
                if (!GeoUtils.isValidLatitude(point.lat())) {
                    throw new QueryShardException(context, "illegal latitude value [{}] for [{}]", point.lat(),
                            GeoPolygonQueryBuilder.NAME);
                }
                if (!GeoUtils.isValidLongitude(point.lat())) {
                    throw new QueryShardException(context, "illegal longitude value [{}] for [{}]", point.lon(),
                            GeoPolygonQueryBuilder.NAME);
                }
            }
        }

        if (GeoValidationMethod.isCoerce(validationMethod)) {
            for (GeoPoint point : shell) {
                GeoUtils.normalizePoint(point, true, true);
            }
        }

        if (context.indexVersionCreated().before(Version.V_2_2_0)) {
            IndexGeoPointFieldData indexFieldData = context.getForField(fieldType);
            return new GeoPolygonQuery(indexFieldData, shell.toArray(new GeoPoint[shellSize]));
        }

        double[] lats = new double[shellSize];
        double[] lons = new double[shellSize];
        GeoPoint p;
        for (int i=0; i<shellSize; ++i) {
            p = new GeoPoint(shell.get(i));
            lats[i] = p.lat();
            lons[i] = p.lon();
        }
        return new GeoPointInPolygonQuery(fieldType.name(), lons, lats);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.startObject(fieldName);
        builder.startArray(GeoPolygonQueryParser.POINTS_FIELD.getPreferredName());
        for (GeoPoint point : shell) {
            builder.startArray().value(point.lon()).value(point.lat()).endArray();
        }
        builder.endArray();
        builder.endObject();

        builder.field(GeoPolygonQueryParser.COERCE_FIELD.getPreferredName(), GeoValidationMethod.isCoerce(validationMethod));
        builder.field(GeoPolygonQueryParser.IGNORE_MALFORMED_FIELD.getPreferredName(), GeoValidationMethod.isIgnoreMalformed(validationMethod));

        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected GeoPolygonQueryBuilder doReadFrom(StreamInput in) throws IOException {
        String fieldName = in.readString();
        List<GeoPoint> shell = new ArrayList<>();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            shell.add(in.readGeoPoint());
        }
        GeoPolygonQueryBuilder builder = new GeoPolygonQueryBuilder(fieldName, shell);
        builder.validationMethod = GeoValidationMethod.readGeoValidationMethodFrom(in);
        return builder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeVInt(shell.size());
        for (GeoPoint point : shell) {
            out.writeGeoPoint(point);
        }
        validationMethod.writeTo(out);
    }

    @Override
    protected boolean doEquals(GeoPolygonQueryBuilder other) {
        return Objects.equals(validationMethod, other.validationMethod)
                && Objects.equals(fieldName, other.fieldName)
                && Objects.equals(shell, other.shell);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(validationMethod, fieldName, shell);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
