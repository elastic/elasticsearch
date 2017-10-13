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

package org.elasticsearch.index.mapper;

import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.locationtech.spatial4j.shape.Point;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.TypeParsers.parseField;

/**
 * This mapper add a new sub fields
 * .bin Binary type
 * .bool Boolean type
 * .point GeoPoint type
 * .shape GeoShape type
 */
public class ExternalMapper extends FieldMapper {

    public static class Names {
        public static final String FIELD_BIN = "bin";
        public static final String FIELD_BOOL = "bool";
        public static final String FIELD_POINT = "point";
        public static final String FIELD_SHAPE = "shape";
    }

    public static class Builder extends FieldMapper.Builder<Builder, ExternalMapper> {

        private BinaryFieldMapper.Builder binBuilder = new BinaryFieldMapper.Builder(Names.FIELD_BIN);
        private BooleanFieldMapper.Builder boolBuilder = new BooleanFieldMapper.Builder(Names.FIELD_BOOL);
        private GeoPointFieldMapper.Builder latLonPointBuilder = new GeoPointFieldMapper.Builder(Names.FIELD_POINT);
        private GeoShapeFieldMapper.Builder shapeBuilder = new GeoShapeFieldMapper.Builder(Names.FIELD_SHAPE);
        private Mapper.Builder stringBuilder;
        private String generatedValue;
        private String mapperName;

        public Builder(String name, String generatedValue, String mapperName) {
            super(name, new ExternalFieldType(), new ExternalFieldType());
            this.builder = this;
            this.stringBuilder = new TextFieldMapper.Builder(name).store(false);
            this.generatedValue = generatedValue;
            this.mapperName = mapperName;
        }

        public Builder string(Mapper.Builder content) {
            this.stringBuilder = content;
            return this;
        }

        @Override
        public ExternalMapper build(BuilderContext context) {
            context.path().add(name);
            BinaryFieldMapper binMapper = binBuilder.build(context);
            BooleanFieldMapper boolMapper = boolBuilder.build(context);
            GeoPointFieldMapper pointMapper = latLonPointBuilder.build(context);
            GeoShapeFieldMapper shapeMapper = shapeBuilder.build(context);
            FieldMapper stringMapper = (FieldMapper)stringBuilder.build(context);
            context.path().remove();

            setupFieldType(context);

            return new ExternalMapper(name, fieldType, generatedValue, mapperName, binMapper, boolMapper, pointMapper, shapeMapper, stringMapper,
                    context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        private String generatedValue;
        private String mapperName;

        TypeParser(String mapperName, String generatedValue) {
            this.mapperName = mapperName;
            this.generatedValue = generatedValue;
        }

        @SuppressWarnings({"unchecked"})
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            ExternalMapper.Builder builder = new ExternalMapper.Builder(name, generatedValue, mapperName);
            parseField(builder, name, node, parserContext);
            return builder;
        }
    }

    static class ExternalFieldType extends TermBasedFieldType {

        ExternalFieldType() {}

        protected ExternalFieldType(ExternalFieldType ref) {
            super(ref);
        }

        @Override
        public MappedFieldType clone() {
            return new ExternalFieldType(this);
        }

        @Override
        public String typeName() {
            return "faketype";
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            if (hasDocValues()) {
                return new DocValuesFieldExistsQuery(name());
            } else {
                return new TermQuery(new Term(FieldNamesFieldMapper.NAME, name()));
            }
        }
    }

    private final String generatedValue;
    private final String mapperName;

    private BinaryFieldMapper binMapper;
    private BooleanFieldMapper boolMapper;
    private GeoPointFieldMapper pointMapper;
    private GeoShapeFieldMapper shapeMapper;
    private FieldMapper stringMapper;

    public ExternalMapper(String simpleName, MappedFieldType fieldType,
                          String generatedValue, String mapperName,
                          BinaryFieldMapper binMapper, BooleanFieldMapper boolMapper, GeoPointFieldMapper pointMapper,
                          GeoShapeFieldMapper shapeMapper, FieldMapper stringMapper, Settings indexSettings, MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, new ExternalFieldType(), indexSettings, multiFields, copyTo);
        this.generatedValue = generatedValue;
        this.mapperName = mapperName;
        this.binMapper = binMapper;
        this.boolMapper = boolMapper;
        this.pointMapper = pointMapper;
        this.shapeMapper = shapeMapper;
        this.stringMapper = stringMapper;
    }

    @Override
    public Mapper parse(ParseContext context) throws IOException {
        byte[] bytes = "Hello world".getBytes(Charset.defaultCharset());
        binMapper.parse(context.createExternalValueContext(bytes));

        boolMapper.parse(context.createExternalValueContext(true));

        // Let's add a Dummy Point
        Double lat = 42.0;
        Double lng = 51.0;
        GeoPoint point = new GeoPoint(lat, lng);
        pointMapper.parse(context.createExternalValueContext(point));

        // Let's add a Dummy Shape
        Point shape = ShapeBuilders.newPoint(-100, 45).build();
        shapeMapper.parse(context.createExternalValueContext(shape));

        context = context.createExternalValueContext(generatedValue);

        // Let's add a Original String
        stringMapper.parse(context);

        multiFields.parse(this, context);
        return null;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void doMerge(Mapper mergeWith, boolean updateAllTypes) {
        // ignore this for now
    }

    @Override
    public FieldMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        ExternalMapper update = (ExternalMapper) super.updateFieldType(fullNameToFieldType);
        MultiFields multiFieldsUpdate = multiFields.updateFieldType(fullNameToFieldType);
        BinaryFieldMapper binMapperUpdate = (BinaryFieldMapper) binMapper.updateFieldType(fullNameToFieldType);
        BooleanFieldMapper boolMapperUpdate = (BooleanFieldMapper) boolMapper.updateFieldType(fullNameToFieldType);
        GeoPointFieldMapper pointMapperUpdate = (GeoPointFieldMapper) pointMapper.updateFieldType(fullNameToFieldType);
        GeoShapeFieldMapper shapeMapperUpdate = (GeoShapeFieldMapper) shapeMapper.updateFieldType(fullNameToFieldType);
        TextFieldMapper stringMapperUpdate = (TextFieldMapper) stringMapper.updateFieldType(fullNameToFieldType);
        if (update == this
                && multiFieldsUpdate == multiFields
                && binMapperUpdate == binMapper
                && boolMapperUpdate == boolMapper
                && pointMapperUpdate == pointMapper
                && shapeMapperUpdate == shapeMapper
                && stringMapperUpdate == stringMapper) {
            return this;
        }
        if (update == this) {
            update = (ExternalMapper) clone();
        }
        update.multiFields = multiFieldsUpdate;
        update.binMapper = binMapperUpdate;
        update.boolMapper = boolMapperUpdate;
        update.pointMapper = pointMapperUpdate;
        update.shapeMapper = shapeMapperUpdate;
        update.stringMapper = stringMapperUpdate;
        return update;
    }

    @Override
    public Iterator<Mapper> iterator() {
        return Iterators.concat(super.iterator(), Arrays.asList(binMapper, boolMapper, pointMapper, shapeMapper, stringMapper).iterator());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(simpleName());
        builder.field("type", mapperName);
        multiFields.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected String contentType() {
        return mapperName;
    }
}
