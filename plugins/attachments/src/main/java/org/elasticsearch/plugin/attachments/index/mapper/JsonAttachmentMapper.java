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

package org.elasticsearch.plugin.attachments.index.mapper;

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.codehaus.jackson.*;
import org.codehaus.jackson.node.ObjectNode;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.index.mapper.json.*;
import org.elasticsearch.util.io.FastByteArrayInputStream;
import org.elasticsearch.util.json.JsonBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static org.elasticsearch.index.mapper.json.JsonMapperBuilders.*;
import static org.elasticsearch.index.mapper.json.JsonTypeParsers.*;
import static org.elasticsearch.plugin.attachments.tika.TikaInstance.*;

/**
 * <pre>
 *      field1 : "..."
 * </pre>
 * <p>Or:
 * <pre>
 * {
 *      file1 : {
 *          _content_type : "application/pdf",
 *          _name : "..../something.pdf",
 *          content : ""
 *      }
 * }
 * </pre>
 *
 * @author kimchy (shay.banon)
 */
public class JsonAttachmentMapper implements JsonMapper {

    public static final String JSON_TYPE = "attachment";

    public static class Defaults {
        public static final JsonPath.Type PATH_TYPE = JsonPath.Type.FULL;
    }

    public static class Builder extends JsonMapper.Builder<Builder, JsonAttachmentMapper> {

        private JsonPath.Type pathType = Defaults.PATH_TYPE;

        private JsonStringFieldMapper.Builder contentBuilder;

        private JsonStringFieldMapper.Builder titleBuilder = stringField("title");

        private JsonStringFieldMapper.Builder authorBuilder = stringField("author");

        private JsonStringFieldMapper.Builder keywordsBuilder = stringField("keywords");

        private JsonDateFieldMapper.Builder dateBuilder = dateField("date");

        public Builder(String name) {
            super(name);
            this.builder = this;
            this.contentBuilder = stringField(name);
        }

        public Builder pathType(JsonPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        public Builder content(JsonStringFieldMapper.Builder content) {
            this.contentBuilder = content;
            return this;
        }

        public Builder date(JsonDateFieldMapper.Builder date) {
            this.dateBuilder = date;
            return this;
        }

        public Builder author(JsonStringFieldMapper.Builder author) {
            this.authorBuilder = author;
            return this;
        }

        public Builder title(JsonStringFieldMapper.Builder title) {
            this.titleBuilder = title;
            return this;
        }

        public Builder keywords(JsonStringFieldMapper.Builder keywords) {
            this.keywordsBuilder = keywords;
            return this;
        }

        @Override public JsonAttachmentMapper build(BuilderContext context) {
            JsonPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            // create the content mapper under the actual name
            JsonStringFieldMapper contentMapper = contentBuilder.build(context);

            // create the DC one under the name
            context.path().add(name);
            JsonDateFieldMapper dateMapper = dateBuilder.build(context);
            JsonStringFieldMapper authorMapper = authorBuilder.build(context);
            JsonStringFieldMapper titleMapper = titleBuilder.build(context);
            JsonStringFieldMapper keywordsMapper = keywordsBuilder.build(context);
            context.path().remove();

            context.path().pathType(origPathType);

            return new JsonAttachmentMapper(name, pathType, contentMapper, dateMapper, titleMapper, authorMapper, keywordsMapper);
        }
    }

    /**
     * <pre>
     *  field1 : { type : "attachment" }
     * </pre>
     * Or:
     * <pre>
     *  field1 : {
     *      type : "attachment",
     *      fields : {
     *          field1 : {type : "binary"},
     *          title : {store : "yes"},
     *          date : {store : "yes"}
     *      }
     * }
     * </pre>
     *
     * @author kimchy (shay.banon)
     */
    public static class TypeParser implements JsonTypeParser {

        @Override public JsonMapper.Builder parse(String name, JsonNode node, ParserContext parserContext) throws MapperParsingException {
            ObjectNode attachmentNode = (ObjectNode) node;
            JsonAttachmentMapper.Builder builder = new JsonAttachmentMapper.Builder(name);

            for (Iterator<Map.Entry<String, JsonNode>> fieldsIt = attachmentNode.getFields(); fieldsIt.hasNext();) {
                Map.Entry<String, JsonNode> entry = fieldsIt.next();
                String fieldName = entry.getKey();
                JsonNode fieldNode = entry.getValue();
                if (fieldName.equals("pathType")) {
                    builder.pathType(parsePathType(name, fieldNode.getValueAsText()));
                } else if (fieldName.equals("fields")) {
                    ObjectNode fieldsNode = (ObjectNode) fieldNode;
                    for (Iterator<Map.Entry<String, JsonNode>> propsIt = fieldsNode.getFields(); propsIt.hasNext();) {
                        Map.Entry<String, JsonNode> entry1 = propsIt.next();
                        String propName = entry1.getKey();
                        JsonNode propNode = entry1.getValue();

                        if (name.equals(propName)) {
                            // that is the content
                            builder.content((JsonStringFieldMapper.Builder) parserContext.typeParser("string").parse(name, propNode, parserContext));
                        } else if ("date".equals(propName)) {
                            builder.date((JsonDateFieldMapper.Builder) parserContext.typeParser("date").parse("date", propNode, parserContext));
                        } else if ("title".equals(propName)) {
                            builder.title((JsonStringFieldMapper.Builder) parserContext.typeParser("string").parse("title", propNode, parserContext));
                        } else if ("author".equals(propName)) {
                            builder.author((JsonStringFieldMapper.Builder) parserContext.typeParser("string").parse("author", propNode, parserContext));
                        } else if ("keywords".equals(propName)) {
                            builder.keywords((JsonStringFieldMapper.Builder) parserContext.typeParser("string").parse("keywords", propNode, parserContext));
                        }
                    }
                }
            }


            return builder;
        }
    }

    private final String name;

    private final JsonPath.Type pathType;

    private final JsonStringFieldMapper contentMapper;

    private final JsonDateFieldMapper dateMapper;

    private final JsonStringFieldMapper authorMapper;

    private final JsonStringFieldMapper titleMapper;

    private final JsonStringFieldMapper keywordsMapper;

    public JsonAttachmentMapper(String name, JsonPath.Type pathType, JsonStringFieldMapper contentMapper,
                                JsonDateFieldMapper dateMapper, JsonStringFieldMapper titleMapper, JsonStringFieldMapper authorMapper,
                                JsonStringFieldMapper keywordsMapper) {
        this.name = name;
        this.pathType = pathType;
        this.contentMapper = contentMapper;
        this.dateMapper = dateMapper;
        this.titleMapper = titleMapper;
        this.authorMapper = authorMapper;
        this.keywordsMapper = keywordsMapper;
    }

    @Override public String name() {
        return name;
    }

    @Override public void parse(JsonParseContext jsonContext) throws IOException {
        byte[] content = null;
        String contentType = null;
        String name = null;
        Base64Variant base64Variant = Base64Variants.getDefaultVariant();

        JsonParser jp = jsonContext.jp();
        JsonToken token = jp.getCurrentToken();
        if (token == JsonToken.VALUE_STRING) {
            content = jp.getBinaryValue();
        } else {
            String currentFieldName = null;
            while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
                if (token == JsonToken.FIELD_NAME) {
                    currentFieldName = jp.getCurrentName();
                } else if (token == JsonToken.VALUE_STRING) {
                    if ("content".equals(currentFieldName)) {
                        content = jp.getBinaryValue(base64Variant);
                    } else if ("_content_type".equals(currentFieldName)) {
                        contentType = jp.getText();
                    } else if ("_name".equals(currentFieldName)) {
                        name = jp.getText();
                    } else if ("_base64".equals(currentFieldName)) {
                        String variant = jp.getText();
                        if ("mime".equals(variant)) {
                            base64Variant = Base64Variants.MIME;
                        } else if ("mime_no_linefeeds".equals(variant)) {
                            base64Variant = Base64Variants.MIME_NO_LINEFEEDS;
                        } else {
                            throw new MapperParsingException("Can't handle base64 [" + variant + "]");
                        }
                    }
                }
            }
        }

        Metadata metadata = new Metadata();
        if (contentType != null) {
            metadata.add(Metadata.CONTENT_TYPE, contentType);
        }
        if (name != null) {
            metadata.add(Metadata.RESOURCE_NAME_KEY, name);
        }

        String parsedContent;
        try {
            parsedContent = tika().parseToString(new FastByteArrayInputStream(content), metadata);
        } catch (TikaException e) {
            throw new MapperParsingException("Failed to extract text for [" + name + "]", e);
        }

        jsonContext.externalValue(parsedContent);
        contentMapper.parse(jsonContext);

        jsonContext.externalValue(metadata.get(Metadata.DATE));
        dateMapper.parse(jsonContext);

        jsonContext.externalValue(metadata.get(Metadata.TITLE));
        titleMapper.parse(jsonContext);

        jsonContext.externalValue(metadata.get(Metadata.AUTHOR));
        authorMapper.parse(jsonContext);

        jsonContext.externalValue(metadata.get(Metadata.KEYWORDS));
        keywordsMapper.parse(jsonContext);
    }

    @Override public void merge(JsonMapper mergeWith, JsonMergeContext mergeContext) throws MergeMappingException {
        // ignore this for now
    }

    @Override public void traverse(FieldMapperListener fieldMapperListener) {
        contentMapper.traverse(fieldMapperListener);
        dateMapper.traverse(fieldMapperListener);
        titleMapper.traverse(fieldMapperListener);
        authorMapper.traverse(fieldMapperListener);
        keywordsMapper.traverse(fieldMapperListener);
    }

    @Override public void toJson(JsonBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", JSON_TYPE);
        builder.field("pathType", pathType.name().toLowerCase());

        builder.startObject("fields");
        contentMapper.toJson(builder, params);
        authorMapper.toJson(builder, params);
        titleMapper.toJson(builder, params);
        dateMapper.toJson(builder, params);
        keywordsMapper.toJson(builder, params);
        builder.endObject();

        builder.endObject();
    }
}
