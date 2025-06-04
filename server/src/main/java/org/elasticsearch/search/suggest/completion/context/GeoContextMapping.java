/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.suggest.completion.context;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.mapper.DocumentParserContext;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static org.elasticsearch.geometry.utils.Geohash.addNeighbors;
import static org.elasticsearch.geometry.utils.Geohash.stringEncode;

/**
 * A {@link ContextMapping} that uses a geo location/area as a
 * criteria.
 * The suggestions can be boosted and/or filtered depending on
 * whether it falls within an area, represented by a query geo hash
 * with a specified precision
 *
 * {@link GeoQueryContext} defines the options for constructing
 * a unit of query context for this context type
 */
public class GeoContextMapping extends ContextMapping<GeoQueryContext> {

    public static final String FIELD_PRECISION = "precision";
    public static final String FIELD_FIELDNAME = "path";

    public static final int DEFAULT_PRECISION = 6;

    static final String CONTEXT_VALUE = "context";
    static final String CONTEXT_BOOST = "boost";
    static final String CONTEXT_PRECISION = "precision";
    static final String CONTEXT_NEIGHBOURS = "neighbours";

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(GeoContextMapping.class);

    private final int precision;
    private final String fieldName;

    private GeoContextMapping(String name, String fieldName, int precision) {
        super(Type.GEO, name);
        this.precision = precision;
        this.fieldName = fieldName;
    }

    public String getFieldName() {
        return fieldName;
    }

    public int getPrecision() {
        return precision;
    }

    protected static GeoContextMapping load(String name, Map<String, Object> config) {
        final GeoContextMapping.Builder builder = new GeoContextMapping.Builder(name);

        if (config != null) {
            final Object configPrecision = config.get(FIELD_PRECISION);
            if (configPrecision != null) {
                if (configPrecision instanceof Integer) {
                    builder.precision((Integer) configPrecision);
                } else if (configPrecision instanceof Long) {
                    builder.precision((Long) configPrecision);
                } else if (configPrecision instanceof Double) {
                    builder.precision((Double) configPrecision);
                } else if (configPrecision instanceof Float) {
                    builder.precision((Float) configPrecision);
                } else {
                    builder.precision(configPrecision.toString());
                }
                config.remove(FIELD_PRECISION);
            }

            final Object fieldName = config.get(FIELD_FIELDNAME);
            if (fieldName != null) {
                builder.field(fieldName.toString());
                config.remove(FIELD_FIELDNAME);
            }
        }
        return builder.build();
    }

    @Override
    protected XContentBuilder toInnerXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(FIELD_PRECISION, precision);
        if (fieldName != null) {
            builder.field(FIELD_FIELDNAME, fieldName);
        }
        return builder;
    }

    /**
     * Parse a set of {@link CharSequence} contexts at index-time.
     * Acceptable formats:
     *
     *  <ul>
     *     <li>Array: <pre>[<i>&lt;GEO POINT&gt;</i>, ..]</pre></li>
     *     <li>String/Object/Array: <pre>&quot;GEO POINT&quot;</pre></li>
     *  </ul>
     *
     * see {@code GeoPoint(String)} for GEO POINT
     */
    @Override
    public Set<String> parseContext(DocumentParserContext documentParserContext, XContentParser parser) throws IOException,
        ElasticsearchParseException {
        final Set<String> contexts = new HashSet<>();
        Token token = parser.currentToken();
        if (token == Token.START_ARRAY) {
            token = parser.nextToken();
            // Test if value is a single point in <code>[lon, lat]</code> format
            if (token == Token.VALUE_NUMBER) {
                double lon = parser.doubleValue();
                if (parser.nextToken() == Token.VALUE_NUMBER) {
                    double lat = parser.doubleValue();
                    if (parser.nextToken() == Token.END_ARRAY) {
                        contexts.add(stringEncode(lon, lat, precision));
                    } else {
                        throw new ElasticsearchParseException("only two values [lon, lat] expected");
                    }
                } else {
                    throw new ElasticsearchParseException("latitude must be a numeric value");
                }
            } else {
                while (token != Token.END_ARRAY) {
                    GeoPoint point = GeoUtils.parseGeoPoint(parser);
                    contexts.add(stringEncode(point.getLon(), point.getLat(), precision));
                    token = parser.nextToken();
                }
            }
        } else if (token == Token.VALUE_STRING) {
            final String geoHash = parser.text();
            final CharSequence truncatedGeoHash = geoHash.subSequence(0, Math.min(geoHash.length(), precision));
            contexts.add(truncatedGeoHash.toString());
        } else {
            // or a single location
            GeoPoint point = GeoUtils.parseGeoPoint(parser);
            contexts.add(stringEncode(point.getLon(), point.getLat(), precision));
        }
        return contexts;
    }

    @Override
    public Set<String> parseContext(LuceneDocument document) {
        final Set<String> geohashes = new HashSet<>();

        if (fieldName != null) {
            List<IndexableField> fields = document.getFields(fieldName);
            GeoPoint spare = new GeoPoint();
            for (IndexableField field : fields) {
                if (field instanceof StringField) {
                    spare.resetFromString(field.stringValue());
                    geohashes.add(spare.geohash());
                } else if (field instanceof LatLonDocValuesField || field instanceof GeoPointFieldMapper.LatLonPointWithDocValues) {
                    spare.resetFromEncoded(field.numericValue().longValue());
                    geohashes.add(spare.geohash());
                } else if (field instanceof LatLonPoint) {
                    BytesRef bytes = field.binaryValue();
                    spare.reset(
                        NumericUtils.sortableBytesToInt(bytes.bytes, bytes.offset),
                        NumericUtils.sortableBytesToInt(bytes.bytes, bytes.offset + Integer.BYTES)
                    );
                    geohashes.add(spare.geohash());
                }
            }
        }

        Set<String> locations = new HashSet<>();
        for (String geohash : geohashes) {
            int precision = Math.min(this.precision, geohash.length());
            String truncatedGeohash = geohash.substring(0, precision);
            locations.add(truncatedGeohash);
        }
        return locations;
    }

    @Override
    protected GeoQueryContext fromXContent(XContentParser parser) throws IOException {
        return GeoQueryContext.fromXContent(parser);
    }

    /**
     * Parse a list of {@link GeoQueryContext}
     * using <code>parser</code>. A QueryContexts accepts one of the following forms:
     *
     * <ul>
     *     <li>Object: GeoQueryContext</li>
     *     <li>String: GeoQueryContext value with boost=1  precision=PRECISION neighbours=[PRECISION]</li>
     *     <li>Array: <pre>[GeoQueryContext, ..]</pre></li>
     * </ul>
     *
     *  A GeoQueryContext has one of the following forms:
     *  <ul>
     *     <li>Object:
     *     <ul>
     *         <li><pre>GEO POINT</pre></li>
     *         <li><pre>{&quot;lat&quot;: <i>&lt;double&gt;</i>, &quot;lon&quot;: <i>&lt;double&gt;</i>, &quot;precision&quot;:
     *         <i>&lt;int&gt;</i>, &quot;neighbours&quot;: <i>&lt;[int, ..]&gt;</i>}</pre></li>
     *         <li><pre>{&quot;context&quot;: <i>&lt;string&gt;</i>, &quot;boost&quot;: <i>&lt;int&gt;</i>, &quot;precision&quot;:
     *         <i>&lt;int&gt;</i>, &quot;neighbours&quot;: <i>&lt;[int, ..]&gt;</i>}</pre></li>
     *         <li><pre>{&quot;context&quot;: <i>&lt;GEO POINT&gt;</i>, &quot;boost&quot;: <i>&lt;int&gt;</i>, &quot;precision&quot;:
     *         <i>&lt;int&gt;</i>, &quot;neighbours&quot;: <i>&lt;[int, ..]&gt;</i>}</pre></li>
     *     </ul>
     *     <li>String: <pre>GEO POINT</pre></li>
     *  </ul>
     * see {@code GeoPoint(String)} for GEO POINT
     */
    @Override
    public List<InternalQueryContext> toInternalQueryContexts(List<GeoQueryContext> queryContexts) {
        List<InternalQueryContext> internalQueryContextList = new ArrayList<>();
        for (GeoQueryContext queryContext : queryContexts) {
            int minPrecision = Math.min(this.precision, queryContext.getPrecision());
            GeoPoint point = queryContext.getGeoPoint();
            final Collection<String> locations = new HashSet<>();
            String geoHash = stringEncode(point.getLon(), point.getLat(), minPrecision);
            locations.add(geoHash);
            if (queryContext.getNeighbours().isEmpty() && geoHash.length() == this.precision) {
                addNeighbors(geoHash, locations);
            } else if (queryContext.getNeighbours().isEmpty() == false) {
                queryContext.getNeighbours()
                    .stream()
                    .filter(neighbourPrecision -> neighbourPrecision < geoHash.length())
                    .forEach(neighbourPrecision -> {
                        String truncatedGeoHash = geoHash.substring(0, neighbourPrecision);
                        locations.add(truncatedGeoHash);
                        addNeighbors(truncatedGeoHash, locations);
                    });
            }
            internalQueryContextList.addAll(
                locations.stream()
                    .map(location -> new InternalQueryContext(location, queryContext.getBoost(), location.length() < this.precision))
                    .toList()
            );
        }
        return internalQueryContextList;
    }

    @Override
    public void validateReferences(IndexVersion indexVersionCreated, Function<String, MappedFieldType> fieldResolver) {
        if (fieldName != null) {
            MappedFieldType mappedFieldType = fieldResolver.apply(fieldName);
            if (mappedFieldType == null) {
                if (indexVersionCreated.before(IndexVersions.V_7_0_0)) {
                    deprecationLogger.warn(
                        DeprecationCategory.MAPPINGS,
                        "geo_context_mapping",
                        "field [{}] referenced in context [{}] is not defined in the mapping",
                        fieldName,
                        name
                    );
                } else {
                    throw new ElasticsearchParseException(
                        "field [{}] referenced in context [{}] is not defined in the mapping",
                        fieldName,
                        name
                    );
                }
            } else if (GeoPointFieldMapper.CONTENT_TYPE.equals(mappedFieldType.typeName()) == false) {
                if (indexVersionCreated.before(IndexVersions.V_7_0_0)) {
                    deprecationLogger.warn(
                        DeprecationCategory.MAPPINGS,
                        "geo_context_mapping",
                        "field [{}] referenced in context [{}] must be mapped to geo_point, found [{}]",
                        fieldName,
                        name,
                        mappedFieldType.typeName()
                    );
                } else {
                    throw new ElasticsearchParseException(
                        "field [{}] referenced in context [{}] must be mapped to geo_point, found [{}]",
                        fieldName,
                        name,
                        mappedFieldType.typeName()
                    );
                }
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        if (super.equals(o) == false) return false;
        GeoContextMapping that = (GeoContextMapping) o;
        if (precision != that.precision) return false;
        return Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), precision, fieldName);
    }

    public static class Builder extends ContextBuilder<GeoContextMapping> {

        private int precision = DEFAULT_PRECISION;
        private String fieldName = null;

        public Builder(String name) {
            super(name);
        }

        /**
         * Set the precision use o make suggestions
         *
         * @param precision
         *            precision as distance with {@link DistanceUnit}. Default:
         *            meters
         * @return this
         */
        public Builder precision(String precision) {
            return precision(DistanceUnit.parse(precision, DistanceUnit.METERS, DistanceUnit.METERS));
        }

        /**
         * Set the precision use o make suggestions
         *
         * @param meters
         *            precision as distance in meters
         * @return this
         */
        public Builder precision(double meters) {
            int level = GeoUtils.geoHashLevelsForPrecision(meters);
            // Ceiling precision: we might return more results
            if (GeoUtils.geoHashCellSize(level) < meters) {
                level = Math.max(1, level - 1);
            }
            return precision(level);
        }

        /**
         * Set the precision use o make suggestions
         *
         * @param level
         *            maximum length of geohashes
         * @return this
         */
        public Builder precision(int level) {
            this.precision = level;
            return this;
        }

        /**
         * Set the name of the field containing a geolocation to use
         * @param fieldName name of the field
         * @return this
         */
        public Builder field(String fieldName) {
            this.fieldName = fieldName;
            return this;
        }

        @Override
        public GeoContextMapping build() {
            return new GeoContextMapping(name, fieldName, precision);
        }
    }
}
