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

import org.apache.lucene.search.Query;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoHashUtils;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.search.geo.GeoPolygonQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class GeoPolygonQueryBuilder extends AbstractQueryBuilder<GeoPolygonQueryBuilder> {

    public static final String NAME = "geo_polygon";

    static final GeoPolygonQueryBuilder PROTOTYPE = new GeoPolygonQueryBuilder("");

    private final String fieldName;

    private final List<GeoPoint> shell;

    private boolean coerce = false;

    private boolean ignoreMalformed = false;

    public GeoPolygonQueryBuilder(String fieldName) {
        this(fieldName, new ArrayList<>());
    }

    public GeoPolygonQueryBuilder(String fieldName, List<GeoPoint> points) {
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName must not be null");
        }
        if (points == null) {
            throw new IllegalArgumentException("polygon must not be null");
        }
        this.fieldName = fieldName;
        this.shell = points;
    }

    public String fieldName() {
        return fieldName;
    }

    /**
     * Adds a point with lat and lon
     *
     * @param lat The latitude
     * @param lon The longitude
     * @return the current builder
     */
    public GeoPolygonQueryBuilder addPoint(double lat, double lon) {
        return addPoint(new GeoPoint(lat, lon));
    }

    public GeoPolygonQueryBuilder addPoint(String geohash) {
        return addPoint(GeoHashUtils.decode(geohash));
    }

    public GeoPolygonQueryBuilder addPoint(GeoPoint point) {
        shell.add(point);
        return this;
    }

    public List<GeoPoint> points() {
        return shell;
    }

    public GeoPolygonQueryBuilder coerce(boolean coerce) {
        if (coerce) {
            this.ignoreMalformed = true;
        }
        this.coerce = coerce;
        return this;
    }

    public boolean coerce() {
        return this.coerce;
    }

    public GeoPolygonQueryBuilder ignoreMalformed(boolean ignoreMalformed) {
        if (coerce == false) {
            this.ignoreMalformed = ignoreMalformed;
        }
        return this;
    }

    public boolean ignoreMalformed() {
        return ignoreMalformed;
    }

    @Override
    public QueryValidationException validate() {
        QueryValidationException errors = null;
        if (fieldName == null) {
            errors = QueryValidationException.addValidationError(NAME, "field must not be null", errors);
        }
        if (shell.isEmpty()) {
            errors = QueryValidationException.addValidationError(NAME, "no points defined for geo_polygon query", errors);
        } else {
            GeoPoint start = shell.get(0);
            if (start.equals(shell.get(shell.size() - 1))) {
                if (shell.size() < 4) {
                    errors = QueryValidationException.addValidationError(NAME, "too few points defined for geo_polygon query", errors);
                }
            } else {
                if (shell.size() < 3) {
                    errors = QueryValidationException.addValidationError(NAME, "too few points defined for geo_polygon query", errors);
                }
            }
        }
        return errors;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {

        if (!shell.get(shell.size() - 1).equals(shell.get(0))) {
            shell.add(shell.get(0));
        }

        final boolean indexCreatedBeforeV2_0 = context.parseContext().shardContext().indexVersionCreated().before(Version.V_2_0_0);
        // validation was not available prior to 2.x, so to support bwc
        // percolation queries we only ignore_malformed on 2.x created indexes
        if (!indexCreatedBeforeV2_0 && !ignoreMalformed) {
            for (GeoPoint point : shell) {
                if (point.lat() > 90.0 || point.lat() < -90.0) {
                    throw new QueryShardException(context, "illegal latitude value [{}] for [{}]", point.lat(),
                            GeoPolygonQueryBuilder.NAME);
                }
                if (point.lon() > 180.0 || point.lon() < -180) {
                    throw new QueryShardException(context, "illegal longitude value [{}] for [{}]", point.lon(),
                            GeoPolygonQueryBuilder.NAME);
                }
            }
        }

        if (coerce) {
            for (GeoPoint point : shell) {
                GeoUtils.normalizePoint(point, coerce, coerce);
            }
        }

        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            throw new QueryShardException(context, "failed to find geo_point field [" + fieldName + "]");
        }
        if (!(fieldType instanceof GeoPointFieldMapper.GeoPointFieldType)) {
            throw new QueryShardException(context, "field [" + fieldName + "] is not a geo_point field");
        }

        IndexGeoPointFieldData indexFieldData = context.getForField(fieldType);
        return new GeoPolygonQuery(indexFieldData, shell.toArray(new GeoPoint[shell.size()]));
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

        builder.field(GeoPolygonQueryParser.COERCE_FIELD.getPreferredName(), coerce);
        builder.field(GeoPolygonQueryParser.IGNORE_MALFORMED_FIELD.getPreferredName(), ignoreMalformed);

        printBoostAndQueryName(builder);
        builder.endObject();
    }

    @Override
    protected GeoPolygonQueryBuilder doReadFrom(StreamInput in) throws IOException {
        String fieldName = in.readString();
        List<GeoPoint> shell = new ArrayList<>();
        int size = in.readVInt();
        for (int i = 0; i < size; i++) {
            shell.add(new GeoPoint(in.readDouble(), in.readDouble()));
        }
        GeoPolygonQueryBuilder builder = new GeoPolygonQueryBuilder(fieldName, shell);
        builder.coerce = in.readBoolean();
        builder.ignoreMalformed = in.readBoolean();
        return builder;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(fieldName);
        out.writeVInt(shell.size());
        for (GeoPoint point : shell) {
            out.writeDouble(point.lat());
            out.writeDouble(point.lon());
        }
        out.writeBoolean(coerce);
        out.writeBoolean(ignoreMalformed);
    }

    @Override
    protected boolean doEquals(GeoPolygonQueryBuilder other) {
        return Objects.equals(coerce, other.coerce)
                && Objects.equals(fieldName, other.fieldName)
                && Objects.equals(ignoreMalformed, other.ignoreMalformed)
                && Objects.equals(shell, other.shell);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(coerce, fieldName, ignoreMalformed, shell);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
