/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.index.mapper.xcontent;

import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.geo.GeoHashUtils;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.elasticsearch.index.mapper.xcontent.XContentTypeParsers.*;

/**
 * Parsing: We handle:
 *
 * - "field" : "geo_hash"
 * - "field" : "lat,lon"
 * - "field" : {
 * "lat" : 1.1,
 * "lon" : 2.1
 * }
 *
 * @author kimchy (shay.banon)
 */
public class XContentGeoPointFieldMapper implements XContentMapper {

    public static final String CONTENT_TYPE = "geo_point";

    public static class Names {
        public static final String LAT = "lat";
        public static final String LAT_SUFFIX = "." + LAT;
        public static final String LON = "lon";
        public static final String LON_SUFFIX = "." + LON;
        public static final String GEOHASH = "geohash";
        public static final String GEOHASH_SUFFIX = "." + GEOHASH;
    }

    public static class Defaults {
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
    }

    public static class Builder extends XContentMapper.Builder<Builder, XContentGeoPointFieldMapper> {

        private ContentPath.Type pathType = Defaults.PATH_TYPE;

        private boolean enableLatLon = true;

        private boolean enableGeohash = false;

        private String resolution = "32";

        private Integer precisionStep;

        private int geohashPrecision = GeoHashUtils.PRECISION;

        public Builder(String name) {
            super(name);
            this.builder = this;
        }

        public Builder pathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        public Builder enableLatLon(boolean enableLatLon) {
            this.enableLatLon = enableLatLon;
            return this;
        }

        public Builder enableGeohash(boolean enableGeohash) {
            this.enableGeohash = enableGeohash;
            return this;
        }

        public Builder resolution(String resolution) {
            this.resolution = resolution;
            return this;
        }

        public Builder precisionStep(int precisionStep) {
            this.precisionStep = precisionStep;
            return this;
        }

        public Builder geohashPrecision(int geohashPrecision) {
            this.geohashPrecision = geohashPrecision;
            return this;
        }

        @Override public XContentGeoPointFieldMapper build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            XContentNumberFieldMapper latMapper = null;
            XContentNumberFieldMapper lonMapper = null;
            XContentStringFieldMapper geohashMapper = null;

            context.path().add(name);
            if (enableLatLon) {
                XContentNumberFieldMapper.Builder latMapperBuilder;
                XContentNumberFieldMapper.Builder lonMapperBuilder;
                if ("32".equals(resolution)) {
                    latMapperBuilder = floatField(Names.LAT).includeInAll(false);
                    lonMapperBuilder = floatField(Names.LON).includeInAll(false);
                } else if ("64".equals(resolution)) {
                    latMapperBuilder = doubleField(Names.LAT).includeInAll(false);
                    lonMapperBuilder = doubleField(Names.LON).includeInAll(false);
                } else {
                    throw new ElasticSearchIllegalArgumentException("Can't handle geo_point resolution [" + resolution + "]");
                }
                if (precisionStep != null) {
                    latMapperBuilder.precisionStep(precisionStep);
                    lonMapperBuilder.precisionStep(precisionStep);
                }
                latMapper = (XContentNumberFieldMapper) latMapperBuilder.includeInAll(false).build(context);
                lonMapper = (XContentNumberFieldMapper) lonMapperBuilder.includeInAll(false).build(context);
            }
            if (enableGeohash) {
                geohashMapper = stringField(Names.GEOHASH).includeInAll(false).build(context);
            }
            context.path().remove();

            context.path().pathType(origPathType);

            return new XContentGeoPointFieldMapper(name, pathType, enableLatLon, enableGeohash, resolution, precisionStep, geohashPrecision, latMapper, lonMapper, geohashMapper);
        }
    }

    public static class TypeParser implements XContentTypeParser {
        @Override public XContentMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            Builder builder = new Builder(name);

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = Strings.toUnderscoreCase(entry.getKey());
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    builder.pathType(parsePathType(name, fieldNode.toString()));
                } else if (fieldName.equals("lat_lon")) {
                    builder.enableLatLon(XContentMapValues.nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("geohash")) {
                    builder.enableGeohash(XContentMapValues.nodeBooleanValue(fieldNode));
                } else if (fieldName.equals("resolution")) {
                    builder.resolution(Integer.toString(XContentMapValues.nodeIntegerValue(fieldNode)));
                } else if (fieldName.equals("precisionStep")) {
                    builder.precisionStep(XContentMapValues.nodeIntegerValue(fieldNode));
                } else if (fieldName.equals("geohash_precision") || fieldName.equals("geohashPrecision")) {
                    builder.geohashPrecision(XContentMapValues.nodeIntegerValue(fieldNode));
                }
            }
            return builder;
        }
    }

    private final String name;

    private final ContentPath.Type pathType;

    private final boolean enableLatLon;

    private final boolean enableGeohash;

    private final String resolution;

    private final Integer precisionStep;

    private final int geohashPrecision;

    private final XContentNumberFieldMapper latMapper;

    private final XContentNumberFieldMapper lonMapper;

    private final XContentStringFieldMapper geohashMapper;

    public XContentGeoPointFieldMapper(String name, ContentPath.Type pathType, boolean enableLatLon, boolean enableGeohash, String resolution, Integer precisionStep, int geohashPrecision,
                                       XContentNumberFieldMapper latMapper, XContentNumberFieldMapper lonMapper, XContentStringFieldMapper geohashMapper) {
        this.name = name;
        this.pathType = pathType;
        this.enableLatLon = enableLatLon;
        this.enableGeohash = enableGeohash;
        this.resolution = resolution;
        this.precisionStep = precisionStep;
        this.geohashPrecision = geohashPrecision;

        this.latMapper = latMapper;
        this.lonMapper = lonMapper;
        this.geohashMapper = geohashMapper;
    }

    @Override public String name() {
        return this.name;
    }

    @Override public void parse(ParseContext context) throws IOException {
        ContentPath.Type origPathType = context.path().pathType();
        context.path().pathType(pathType);
        context.path().add(name);

        XContentParser.Token token = context.parser().currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            String value = context.parser().text();
            int comma = value.indexOf(',');
            if (comma != -1) {
                double lat = Double.parseDouble(value.substring(0, comma).trim());
                double lon = Double.parseDouble(value.substring(comma + 1).trim());
                parseLatLon(context, lat, lon);
            } else {
                // geo hash
                parseGeohash(context, value);
            }
        } else if (token == XContentParser.Token.START_OBJECT) {
            String currentName = context.parser().currentName();
            Double lat = null;
            Double lon = null;
            String geohash = null;
            while ((token = context.parser().nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentName = context.parser().currentName();
                } else if (token.isValue()) {
                    if (currentName.equals(Names.LAT)) {
                        lat = context.parser().doubleValue();
                    } else if (currentName.equals(Names.LON)) {
                        lon = context.parser().doubleValue();
                    } else if (currentName.equals(Names.GEOHASH)) {
                        geohash = context.parser().text();
                    }
                }
            }
            if (geohash != null) {
                parseGeohash(context, geohash);
            } else if (lat != null && lon != null) {
                parseLatLon(context, lat, lon);
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            token = context.parser().nextToken();
            Double lat = context.parser().doubleValue();
            token = context.parser().nextToken();
            Double lon = context.parser().doubleValue();
            token = context.parser().nextToken();
            while ((token = context.parser().nextToken()) != XContentParser.Token.END_ARRAY) {

            }
            parseLatLon(context, lat, lon);
        }

        context.path().remove();
        context.path().pathType(origPathType);
    }

    private void parseLatLon(ParseContext context, Double lat, Double lon) throws IOException {
        if (enableLatLon) {
            context.externalValue(lat);
            latMapper.parse(context);
            context.externalValue(lon);
            lonMapper.parse(context);
        }
        if (enableGeohash) {
            context.externalValue(GeoHashUtils.encode(lat, lon, geohashPrecision));
            geohashMapper.parse(context);
        }
    }

    private void parseGeohash(ParseContext context, String geohash) throws IOException {
        if (enableLatLon) {
            double[] values = GeoHashUtils.decode(geohash);
            context.externalValue(values[0]);
            latMapper.parse(context);
            context.externalValue(values[1]);
            lonMapper.parse(context);
        }
        if (enableGeohash) {
            context.externalValue(geohash);
            geohashMapper.parse(context);
        }
    }

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // TODO
    }

    @Override public void traverse(FieldMapperListener fieldMapperListener) {
        if (enableLatLon) {
            latMapper.traverse(fieldMapperListener);
            lonMapper.traverse(fieldMapperListener);
        }
        if (enableGeohash) {
            geohashMapper.traverse(fieldMapperListener);
        }
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", CONTENT_TYPE);
        builder.field("path", pathType.name().toLowerCase());
        builder.field("lat_lon", enableLatLon);
        builder.field("geohash", enableGeohash);
        builder.field("resolution", resolution);
        builder.field("geohash_precision", geohashPrecision);
        if (precisionStep != null) {
            builder.field("precision_step", precisionStep);
        }

        builder.endObject();
    }
}
