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

import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.elasticsearch.common.io.FastByteArrayInputStream;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.MergeMappingException;

import java.io.IOException;
import java.util.Map;

import static org.elasticsearch.index.mapper.xcontent.XContentMapperBuilders.*;
import static org.elasticsearch.index.mapper.xcontent.XContentTypeParsers.*;
import static org.elasticsearch.plugin.mapper.attachments.tika.TikaInstance.*;

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
public class AttachmentMapper implements XContentMapper {

    public static final String CONTENT_TYPE = "attachment";

    public static class Defaults {
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
    }

    public static class Builder extends XContentMapper.Builder<Builder, AttachmentMapper> {

        private ContentPath.Type pathType = Defaults.PATH_TYPE;

        private StringFieldMapper.Builder contentBuilder;

        private StringFieldMapper.Builder titleBuilder = stringField("title");

        private StringFieldMapper.Builder authorBuilder = stringField("author");

        private StringFieldMapper.Builder keywordsBuilder = stringField("keywords");

        private DateFieldMapper.Builder dateBuilder = dateField("date");

        public Builder(String name) {
            super(name);
            this.builder = this;
            this.contentBuilder = stringField(name);
        }

        public Builder pathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        public Builder content(StringFieldMapper.Builder content) {
            this.contentBuilder = content;
            return this;
        }

        public Builder date(DateFieldMapper.Builder date) {
            this.dateBuilder = date;
            return this;
        }

        public Builder author(StringFieldMapper.Builder author) {
            this.authorBuilder = author;
            return this;
        }

        public Builder title(StringFieldMapper.Builder title) {
            this.titleBuilder = title;
            return this;
        }

        public Builder keywords(StringFieldMapper.Builder keywords) {
            this.keywordsBuilder = keywords;
            return this;
        }

        @Override public AttachmentMapper build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            // create the content mapper under the actual name
            StringFieldMapper contentMapper = contentBuilder.build(context);

            // create the DC one under the name
            context.path().add(name);
            DateFieldMapper dateMapper = dateBuilder.build(context);
            StringFieldMapper authorMapper = authorBuilder.build(context);
            StringFieldMapper titleMapper = titleBuilder.build(context);
            StringFieldMapper keywordsMapper = keywordsBuilder.build(context);
            context.path().remove();

            context.path().pathType(origPathType);

            return new AttachmentMapper(name, pathType, contentMapper, dateMapper, titleMapper, authorMapper, keywordsMapper);
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
    public static class TypeParser implements XContentMapper.TypeParser {

        @SuppressWarnings({"unchecked"}) @Override public XContentMapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            AttachmentMapper.Builder builder = new AttachmentMapper.Builder(name);

            for (Map.Entry<String, Object> entry : node.entrySet()) {
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (fieldName.equals("path")) {
                    builder.pathType(parsePathType(name, fieldNode.toString()));
                } else if (fieldName.equals("fields")) {
                    Map<String, Object> fieldsNode = (Map<String, Object>) fieldNode;
                    for (Map.Entry<String, Object> entry1 : fieldsNode.entrySet()) {
                        String propName = entry1.getKey();
                        Object propNode = entry1.getValue();

                        if (name.equals(propName)) {
                            // that is the content
                            builder.content((StringFieldMapper.Builder) parserContext.typeParser("string").parse(name, (Map<String, Object>) propNode, parserContext));
                        } else if ("date".equals(propName)) {
                            builder.date((DateFieldMapper.Builder) parserContext.typeParser("date").parse("date", (Map<String, Object>) propNode, parserContext));
                        } else if ("title".equals(propName)) {
                            builder.title((StringFieldMapper.Builder) parserContext.typeParser("string").parse("title", (Map<String, Object>) propNode, parserContext));
                        } else if ("author".equals(propName)) {
                            builder.author((StringFieldMapper.Builder) parserContext.typeParser("string").parse("author", (Map<String, Object>) propNode, parserContext));
                        } else if ("keywords".equals(propName)) {
                            builder.keywords((StringFieldMapper.Builder) parserContext.typeParser("string").parse("keywords", (Map<String, Object>) propNode, parserContext));
                        }
                    }
                }
            }

            return builder;
        }
    }

    private final String name;

    private final ContentPath.Type pathType;

    private final StringFieldMapper contentMapper;

    private final DateFieldMapper dateMapper;

    private final StringFieldMapper authorMapper;

    private final StringFieldMapper titleMapper;

    private final StringFieldMapper keywordsMapper;

    public AttachmentMapper(String name, ContentPath.Type pathType, StringFieldMapper contentMapper,
                            DateFieldMapper dateMapper, StringFieldMapper titleMapper, StringFieldMapper authorMapper,
                            StringFieldMapper keywordsMapper) {
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

    @Override public void parse(ParseContext context) throws IOException {
        byte[] content = null;
        String contentType = null;
        String name = null;

        XContentParser parser = context.parser();
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.VALUE_STRING) {
            content = parser.binaryValue();
        } else {
            String currentFieldName = null;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (token == XContentParser.Token.VALUE_STRING) {
                    if ("content".equals(currentFieldName)) {
                        content = parser.binaryValue();
                    } else if ("_content_type".equals(currentFieldName)) {
                        contentType = parser.text();
                    } else if ("_name".equals(currentFieldName)) {
                        name = parser.text();
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

        context.externalValue(parsedContent);
        contentMapper.parse(context);

        context.externalValue(metadata.get(Metadata.DATE));
        dateMapper.parse(context);

        context.externalValue(metadata.get(Metadata.TITLE));
        titleMapper.parse(context);

        context.externalValue(metadata.get(Metadata.AUTHOR));
        authorMapper.parse(context);

        context.externalValue(metadata.get(Metadata.KEYWORDS));
        keywordsMapper.parse(context);
    }

    @Override public void merge(XContentMapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // ignore this for now
    }

    @Override public void traverse(FieldMapperListener fieldMapperListener) {
        contentMapper.traverse(fieldMapperListener);
        dateMapper.traverse(fieldMapperListener);
        titleMapper.traverse(fieldMapperListener);
        authorMapper.traverse(fieldMapperListener);
        keywordsMapper.traverse(fieldMapperListener);
    }

    @Override public void close() {
        contentMapper.close();
        dateMapper.close();
        titleMapper.close();
        authorMapper.close();
        keywordsMapper.close();
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("type", CONTENT_TYPE);
        builder.field("path", pathType.name().toLowerCase());

        builder.startObject("fields");
        contentMapper.toXContent(builder, params);
        authorMapper.toXContent(builder, params);
        titleMapper.toXContent(builder, params);
        dateMapper.toXContent(builder, params);
        keywordsMapper.toXContent(builder, params);
        builder.endObject();

        builder.endObject();
        return builder;
    }
}
