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

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.spatial.geopoint.document.GeoPointField;
import org.apache.lucene.spatial.geopoint.search.GeoPointInPolygonQuery;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.index.fielddata.IndexGeoPointFieldData;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.geo.BaseGeoPointFieldMapper;
import org.elasticsearch.index.search.geo.GeoPolygonQuery;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class GeoPolygonQueryBuilder extends AbstractQueryBuilder<GeoPolygonQueryBuilder> {

    public static final String NAME = "geo_polygon";
    public static final ParseField QUERY_NAME_FIELD = new ParseField(NAME);

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    private static final ParseField COERCE_FIELD = new ParseField("coerce", "normalize")
            .withAllDeprecated("use validation_method instead");
    private static final ParseField IGNORE_MALFORMED_FIELD = new ParseField("ignore_malformed")
            .withAllDeprecated("use validation_method instead");
    private static final ParseField VALIDATION_METHOD = new ParseField("validation_method");
    private static final ParseField POINTS_FIELD = new ParseField("points");
    private static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    private final String fieldName;

    private final List<GeoPoint> shell;

    private GeoValidationMethod validationMethod = GeoValidationMethod.DEFAULT;

    private boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

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
        out.writeVInt(shell.size());
        for (GeoPoint point : shell) {
            out.writeGeoPoint(point);
        }
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

    /** Returns the validation method to use for geo coordinates. */
    public GeoValidationMethod getValidationMethod() {
        return this.validationMethod;
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

    /**
     * Gets whether the query builder will ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "failed to find geo_point field [" + fieldName + "]");
            }
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

        final Version indexVersionCreated = context.indexVersionCreated();
        if (indexVersionCreated.onOrAfter(Version.V_2_2_0) || GeoValidationMethod.isCoerce(validationMethod)) {
            for (GeoPoint point : shell) {
                GeoUtils.normalizePoint(point, true, true);
            }
        }

        if (indexVersionCreated.before(Version.V_2_2_0)) {
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
        // if index created V_2_2 use (soon to be legacy) numeric encoding postings format
        // if index created V_2_3 > use prefix encoded postings format
        final GeoPointField.TermEncoding encoding = (indexVersionCreated.before(Version.V_2_3_0)) ?
            GeoPointField.TermEncoding.NUMERIC : GeoPointField.TermEncoding.PREFIX;
        return new GeoPointInPolygonQuery(fieldType.name(), encoding, lats, lons);
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

    public static Optional<GeoPolygonQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        String fieldName = null;

        List<GeoPoint> shell = null;

        Float boost = null;
        boolean coerce = GeoValidationMethod.DEFAULT_LENIENT_PARSING;
        boolean ignoreMalformed = GeoValidationMethod.DEFAULT_LENIENT_PARSING;
        GeoValidationMethod validationMethod = null;
        String queryName = null;
        String currentFieldName = null;
        XContentParser.Token token;
        boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (parseContext.isDeprecatedSetting(currentFieldName)) {
                // skip
            } else if (token == XContentParser.Token.START_OBJECT) {
                fieldName = currentFieldName;

                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                    } else if (token == XContentParser.Token.START_ARRAY) {
                        if (parseContext.getParseFieldMatcher().match(currentFieldName, POINTS_FIELD)) {
                            shell = new ArrayList<GeoPoint>();
                            while ((token = parser.nextToken()) != Token.END_ARRAY) {
                                shell.add(GeoUtils.parseGeoPoint(parser));
                            }
                        } else {
                            throw new ParsingException(parser.getTokenLocation(),
                                    "[geo_polygon] query does not support [" + currentFieldName + "]");
                        }
                    } else {
                        throw new ParsingException(parser.getTokenLocation(),
                                "[geo_polygon] query does not support token type [" + token.name() + "] under [" + currentFieldName + "]");
                    }
                }
            } else if (token.isValue()) {
                if ("_name".equals(currentFieldName)) {
                    queryName = parser.text();
                } else if ("boost".equals(currentFieldName)) {
                    boost = parser.floatValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, COERCE_FIELD)) {
                    coerce = parser.booleanValue();
                    if (coerce) {
                        ignoreMalformed = true;
                    }
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, IGNORE_UNMAPPED_FIELD)) {
                    ignoreUnmapped = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, IGNORE_MALFORMED_FIELD)) {
                    ignoreMalformed = parser.booleanValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, VALIDATION_METHOD)) {
                    validationMethod = GeoValidationMethod.fromString(parser.text());
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "[geo_polygon] query does not support [" + currentFieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[geo_polygon] unexpected token type [" + token.name() + "]");
            }
        }
        GeoPolygonQueryBuilder builder = new GeoPolygonQueryBuilder(fieldName, shell);
        if (validationMethod != null) {
            // if GeoValidationMethod was explicitly set ignore deprecated coerce and ignoreMalformed settings
            builder.setValidationMethod(validationMethod);
        } else {
            builder.setValidationMethod(GeoValidationMethod.infer(coerce, ignoreMalformed));
        }

        if (queryName != null) {
            builder.queryName(queryName);
        }
        if (boost != null) {
            builder.boost(boost);
        }
        builder.ignoreUnmapped(ignoreUnmapped);
        return Optional.of(builder);
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
}
