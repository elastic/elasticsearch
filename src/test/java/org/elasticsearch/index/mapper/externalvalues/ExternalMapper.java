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

package org.elasticsearch.index.mapper.externalvalues;

import com.spatial4j.core.shape.Point;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.BinaryFieldMapper;
import org.elasticsearch.index.mapper.core.BooleanFieldMapper;
import org.elasticsearch.index.mapper.core.StringFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.stringField;

/**
 * This mapper add a new sub fields
 * .bin Binary type
 * .bool Boolean type
 * .point GeoPoint type
 * .shape GeoShape type
 */
public class ExternalMapper implements Mapper {
    public static class Names {
        public static final String FIELD_BIN = "bin";
        public static final String FIELD_BOOL = "bool";
        public static final String FIELD_POINT = "point";
        public static final String FIELD_SHAPE = "shape";
    }

    public static class Builder extends Mapper.Builder<Builder, ExternalMapper> {

        private BinaryFieldMapper.Builder binBuilder = new BinaryFieldMapper.Builder(Names.FIELD_BIN);
        private BooleanFieldMapper.Builder boolBuilder = new BooleanFieldMapper.Builder(Names.FIELD_BOOL);
        private GeoPointFieldMapper.Builder pointBuilder = new GeoPointFieldMapper.Builder(Names.FIELD_POINT);
        private GeoShapeFieldMapper.Builder shapeBuilder = new GeoShapeFieldMapper.Builder(Names.FIELD_SHAPE);
        private Mapper.Builder stringBuilder;

        public Builder(String name) {
            super(name);
            this.builder = this;
            this.stringBuilder = stringField(name).store(false);
        }

        public Builder string(Mapper.Builder content) {
            this.stringBuilder = content;
            return this;
        }

        @Override
        public ExternalMapper build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(ContentPath.Type.FULL);

            context.path().add(name);
            BinaryFieldMapper binMapper = binBuilder.build(context);
            BooleanFieldMapper boolMapper = boolBuilder.build(context);
            GeoPointFieldMapper pointMapper = pointBuilder.build(context);
            GeoShapeFieldMapper shapeMapper = shapeBuilder.build(context);
            Mapper stringMapper = stringBuilder.build(context);
            context.path().remove();

            context.path().pathType(origPathType);

            return new ExternalMapper(name, binMapper, boolMapper, pointMapper, shapeMapper, stringMapper);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {

        @SuppressWarnings({"unchecked"})
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            ExternalMapper.Builder builder = new ExternalMapper.Builder(name);

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("fields")) {
                    Map<String, Object> fieldsNode = (Map<String, Object>) fieldNode;
                    for (Map.Entry<String, Object> entry1 : fieldsNode.entrySet()) {
                        String propName = entry1.getKey();
                        Object propNode = entry1.getValue();
                        if (name.equals(propName)) {
                            builder.string(parserContext.typeParser(StringFieldMapper.CONTENT_TYPE).parse(name, (Map<String, Object>) propNode, parserContext));
                        }
                    }
                }
            }

            return builder;
        }
    }

    private final String name;

    private final BinaryFieldMapper binMapper;
    private final BooleanFieldMapper boolMapper;
    private final GeoPointFieldMapper pointMapper;
    private final GeoShapeFieldMapper shapeMapper;
    private final Mapper stringMapper;

    public ExternalMapper(String name,
                          BinaryFieldMapper binMapper, BooleanFieldMapper boolMapper, GeoPointFieldMapper pointMapper,
                          GeoShapeFieldMapper shapeMapper, Mapper stringMapper) {
        this.name = name;
        this.binMapper = binMapper;
        this.boolMapper = boolMapper;
        this.pointMapper = pointMapper;
        this.shapeMapper = shapeMapper;
        this.stringMapper = stringMapper;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        XContentParser parser = context.parser();
        String content = parser.text();

        context.path().add(name);

        byte[] bytes = "Hello world".getBytes(Charset.defaultCharset());
        context.externalValue(bytes);
        binMapper.parse(context);

        context.externalValue(true);
        boolMapper.parse(context);

        // Let's add a Dummy Point
        Double lat = 42.0;
        Double lng = 51.0;
        GeoPoint point = new GeoPoint(lat, lng);
        context.externalValue(point);
        pointMapper.parse(context);

        // Let's add a Dummy Shape
        Point shape = ShapeBuilder.newPoint(-100, 45).build();
        context.externalValue(shape);
        shapeMapper.parse(context);

        // Let's add a Dummy String
        context.externalValue("dummy");
        stringMapper.parse(context);
    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // ignore this for now
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        binMapper.traverse(fieldMapperListener);
        boolMapper.traverse(fieldMapperListener);
        pointMapper.traverse(fieldMapperListener);
        shapeMapper.traverse(fieldMapperListener);
        stringMapper.traverse(fieldMapperListener);
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
    }

    @Override
    public void close() {
        binMapper.close();
        boolMapper.close();
        pointMapper.close();
        shapeMapper.close();
        stringMapper.close();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", RegisterExternalTypes.EXTERNAL);
        builder.startObject("fields");
        binMapper.toXContent(builder, params);
        boolMapper.toXContent(builder, params);
        pointMapper.toXContent(builder, params);
        shapeMapper.toXContent(builder, params);
        stringMapper.toXContent(builder, params);
        builder.endObject();

        builder.endObject();
        return builder;
    }
}
