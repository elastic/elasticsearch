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

package org.elasticsearch.index.mapper.attachment;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.tika.language.LanguageIdentifier;
import org.apache.tika.metadata.Metadata;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.core.AbstractFieldMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.MapperBuilders.*;
import static org.elasticsearch.index.mapper.core.TypeParsers.parseMultiField;
import static org.elasticsearch.index.mapper.core.TypeParsers.parsePathType;
import static org.elasticsearch.plugin.mapper.attachments.tika.TikaInstance.tika;

/**
 * <pre>
 *      "field1" : "..."
 * </pre>
 * <p>Or:
 * <pre>
 * {
 *      "file1" : {
 *          "_content_type" : "application/pdf",
 *          "_content_length" : "500000000",
 *          "_name" : "..../something.pdf",
 *          "_content" : ""
 *      }
 * }
 * </pre>
 * <p/>
 * _content_length = Specify the maximum amount of characters to extract from the attachment. If not specified, then the default for
 * tika is 100,000 characters. Caution is required when setting large values as this can cause memory issues.
 */
public class AttachmentMapper extends AbstractFieldMapper<Object> {

    private static ESLogger logger = ESLoggerFactory.getLogger(AttachmentMapper.class.getName());

    public static final String CONTENT_TYPE = "attachment";

    public static class Defaults {
        public static final ContentPath.Type PATH_TYPE = ContentPath.Type.FULL;
    }

    public static class FieldNames {
        public static final String TITLE = "title";
        public static final String NAME = "name";
        public static final String AUTHOR = "author";
        public static final String KEYWORDS = "keywords";
        public static final String DATE = "date";
        public static final String CONTENT_TYPE = "content_type";
        public static final String CONTENT_LENGTH = "content_length";
        public static final String LANGUAGE = "language";
    }

    public static class Builder extends AbstractFieldMapper.Builder<Builder, AttachmentMapper> {

        private ContentPath.Type pathType = Defaults.PATH_TYPE;

        private Boolean ignoreErrors = null;

        private Integer defaultIndexedChars = null;

        private Boolean langDetect = null;

        private Mapper.Builder contentBuilder;

        private Mapper.Builder titleBuilder = stringField(FieldNames.TITLE);

        private Mapper.Builder nameBuilder = stringField(FieldNames.NAME);

        private Mapper.Builder authorBuilder = stringField(FieldNames.AUTHOR);

        private Mapper.Builder keywordsBuilder = stringField(FieldNames.KEYWORDS);

        private Mapper.Builder dateBuilder = dateField(FieldNames.DATE);

        private Mapper.Builder contentTypeBuilder = stringField(FieldNames.CONTENT_TYPE);

        private Mapper.Builder contentLengthBuilder = integerField(FieldNames.CONTENT_LENGTH);

        private Mapper.Builder languageBuilder = stringField(FieldNames.LANGUAGE);

        public Builder(String name) {
            super(name, new FieldType(AbstractFieldMapper.Defaults.FIELD_TYPE));
            this.builder = this;
            this.contentBuilder = stringField(name);
        }

        public Builder pathType(ContentPath.Type pathType) {
            this.pathType = pathType;
            return this;
        }

        public Builder content(Mapper.Builder content) {
            this.contentBuilder = content;
            return this;
        }

        public Builder date(Mapper.Builder date) {
            this.dateBuilder = date;
            return this;
        }

        public Builder author(Mapper.Builder author) {
            this.authorBuilder = author;
            return this;
        }

        public Builder title(Mapper.Builder title) {
            this.titleBuilder = title;
            return this;
        }

        public Builder name(Mapper.Builder name) {
            this.nameBuilder = name;
            return this;
        }

        public Builder keywords(Mapper.Builder keywords) {
            this.keywordsBuilder = keywords;
            return this;
        }

        public Builder contentType(Mapper.Builder contentType) {
            this.contentTypeBuilder = contentType;
            return this;
        }

        public Builder contentLength(Mapper.Builder contentType) {
            this.contentLengthBuilder = contentType;
            return this;
        }

        public Builder language(Mapper.Builder language) {
            this.languageBuilder = language;
            return this;
        }

        @Override
        public AttachmentMapper build(BuilderContext context) {
            ContentPath.Type origPathType = context.path().pathType();
            context.path().pathType(pathType);

            // create the content mapper under the actual name
            Mapper contentMapper = contentBuilder.build(context);

            // create the DC one under the name
            context.path().add(name);
            Mapper dateMapper = dateBuilder.build(context);
            Mapper authorMapper = authorBuilder.build(context);
            Mapper titleMapper = titleBuilder.build(context);
            Mapper nameMapper = nameBuilder.build(context);
            Mapper keywordsMapper = keywordsBuilder.build(context);
            Mapper contentTypeMapper = contentTypeBuilder.build(context);
            Mapper contentLength = contentLengthBuilder.build(context);
            Mapper language = languageBuilder.build(context);
            context.path().remove();

            context.path().pathType(origPathType);

            if (defaultIndexedChars == null && context.indexSettings() != null) {
                defaultIndexedChars = context.indexSettings().getAsInt("index.mapping.attachment.indexed_chars", 100000);
            }
            if (defaultIndexedChars == null) {
                defaultIndexedChars = 100000;
            }

            if (ignoreErrors == null && context.indexSettings() != null) {
                ignoreErrors = context.indexSettings().getAsBoolean("index.mapping.attachment.ignore_errors", Boolean.TRUE);
            }
            if (ignoreErrors == null) {
                ignoreErrors = Boolean.TRUE;
            }

            if (langDetect == null && context.indexSettings() != null) {
                langDetect = context.indexSettings().getAsBoolean("index.mapping.attachment.detect_language", Boolean.FALSE);
            }
            if (langDetect == null) {
                langDetect = Boolean.FALSE;
            }

            return new AttachmentMapper(buildNames(context), pathType, defaultIndexedChars, ignoreErrors, langDetect, contentMapper,
                    dateMapper, titleMapper, nameMapper, authorMapper, keywordsMapper, contentTypeMapper, contentLength,
                    language, multiFieldsBuilder.build(this, context), copyTo);
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
     *          date : {store : "yes"},
     *          name : {store : "yes"},
     *          author : {store : "yes"},
     *          keywords : {store : "yes"},
     *          content_type : {store : "yes"},
     *          content_length : {store : "yes"}
     *      }
     * }
     * </pre>
     */
    public static class TypeParser implements Mapper.TypeParser {

        private Mapper.Builder<?, ?> findMapperBuilder(Map<String, Object> propNode, String propName, ParserContext parserContext) {
            String type;
            Object typeNode = propNode.get("type");
            if (typeNode != null) {
                type = typeNode.toString();
            } else {
                type = "string";
            }
            Mapper.TypeParser typeParser = parserContext.typeParser(type);
            Mapper.Builder<?, ?> mapperBuilder = typeParser.parse(propName, (Map<String, Object>) propNode, parserContext);

            return mapperBuilder;
        }

        @SuppressWarnings({"unchecked"})
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
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
                        Map<String, Object> propNode = (Map<String, Object>) entry1.getValue();

                        Mapper.Builder<?, ?> mapperBuilder = findMapperBuilder(propNode, propName, parserContext);
                        parseMultiField((AbstractFieldMapper.Builder) mapperBuilder, fieldName, (Map<String, Object>) fieldNode, parserContext, propName, propNode);

                        if (propName.equals(name)) {
                            builder.content(mapperBuilder);
                        } else {
                            switch (propName) {
                                case FieldNames.DATE:
                                    builder.date(mapperBuilder);
                                    break;
                                case FieldNames.AUTHOR:
                                    builder.author(mapperBuilder);
                                    break;
                                case FieldNames.CONTENT_LENGTH:
                                    builder.contentLength(mapperBuilder);
                                    break;
                                case FieldNames.CONTENT_TYPE:
                                    builder.contentType(mapperBuilder);
                                    break;
                                case FieldNames.KEYWORDS:
                                    builder.keywords(mapperBuilder);
                                    break;
                                case FieldNames.LANGUAGE:
                                    builder.language(mapperBuilder);
                                    break;
                                case FieldNames.TITLE:
                                    builder.title(mapperBuilder);
                                    break;
                                case FieldNames.NAME:
                                    builder.name(mapperBuilder);
                                    break;
                            }
                        }
                    }
                }
            }

            return builder;
        }
    }

    private final ContentPath.Type pathType;

    private final int defaultIndexedChars;

    private final boolean ignoreErrors;

    private final boolean defaultLangDetect;

    private final Mapper contentMapper;

    private final Mapper dateMapper;

    private final Mapper authorMapper;

    private final Mapper titleMapper;

    private final Mapper nameMapper;

    private final Mapper keywordsMapper;

    private final Mapper contentTypeMapper;

    private final Mapper contentLengthMapper;

    private final Mapper languageMapper;

    public AttachmentMapper(Names names, ContentPath.Type pathType, int defaultIndexedChars, Boolean ignoreErrors,
                            Boolean defaultLangDetect, Mapper contentMapper,
                            Mapper dateMapper, Mapper titleMapper, Mapper nameMapper, Mapper authorMapper,
                            Mapper keywordsMapper, Mapper contentTypeMapper, Mapper contentLengthMapper,
                            Mapper languageMapper, MultiFields multiFields, CopyTo copyTo) {
        super(names, 1.0f, AbstractFieldMapper.Defaults.FIELD_TYPE, false, null, null, null, null, null, null, null,
                ImmutableSettings.EMPTY, multiFields, copyTo);
        this.pathType = pathType;
        this.defaultIndexedChars = defaultIndexedChars;
        this.ignoreErrors = ignoreErrors;
        this.defaultLangDetect = defaultLangDetect;
        this.contentMapper = contentMapper;
        this.dateMapper = dateMapper;
        this.titleMapper = titleMapper;
        this.nameMapper = nameMapper;
        this.authorMapper = authorMapper;
        this.keywordsMapper = keywordsMapper;
        this.contentTypeMapper = contentTypeMapper;
        this.contentLengthMapper = contentLengthMapper;
        this.languageMapper = languageMapper;
    }

    @Override
    public Object value(Object value) {
        return null;
    }

    @Override
    public FieldType defaultFieldType() {
        return AbstractFieldMapper.Defaults.FIELD_TYPE;
    }

    @Override
    public FieldDataType defaultFieldDataType() {
        return null;
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        byte[] content = null;
        String contentType = null;
        int indexedChars = defaultIndexedChars;
        boolean langDetect = defaultLangDetect;
        String name = null;
        String language = null;

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
                    if ("_content".equals(currentFieldName)) {
                        content = parser.binaryValue();
                    } else if ("content".equals(currentFieldName)) {
                        // TODO Remove in 2.4.0. See #75 https://github.com/elasticsearch/elasticsearch-mapper-attachments/issues/75
                        logger.warn("`content` has been deprecated by _content. Please update your code. Will be removed in a future version.");
                        content = parser.binaryValue();
                    } else if ("_content_type".equals(currentFieldName)) {
                        contentType = parser.text();
                    } else if ("_name".equals(currentFieldName)) {
                        name = parser.text();
                    } else if ("_language".equals(currentFieldName)) {
                        language = parser.text();
                    }
                } else if (token == XContentParser.Token.VALUE_NUMBER) {
                    if ("_indexed_chars".equals(currentFieldName) || "_indexedChars".equals(currentFieldName)) {
                        indexedChars = parser.intValue();
                    }
                } else if (token == XContentParser.Token.VALUE_BOOLEAN) {
                    if ("_detect_language".equals(currentFieldName) || "_detectLanguage".equals(currentFieldName)) {
                        langDetect = parser.booleanValue();
                    }
                }
            }
        }

        // Throw clean exception when no content is provided Fix #23
        if (content == null) {
            throw new MapperParsingException("No content is provided.");
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
            // Set the maximum length of strings returned by the parseToString method, -1 sets no limit
            parsedContent = tika().parseToString(new BytesStreamInput(content, false), metadata, indexedChars);
        } catch (Throwable e) {
            // #18: we could ignore errors when Tika does not parse data
            if (!ignoreErrors) {
                throw new MapperParsingException("Failed to extract [" + indexedChars + "] characters of text for [" + name + "]", e);
            } else {
                logger.debug("Failed to extract [{}] characters of text for [{}]: [{}]", indexedChars, name, e.getMessage());
            }
            return;
        }

        context = context.createExternalValueContext(parsedContent);
        contentMapper.parse(context);

        if (langDetect) {
            try {
                if (language != null) {
                    metadata.add(Metadata.CONTENT_LANGUAGE, language);
                } else {
                    LanguageIdentifier identifier = new LanguageIdentifier(parsedContent);
                    language = identifier.getLanguage();
                }
                context = context.createExternalValueContext(language);
                languageMapper.parse(context);
            } catch(Throwable t) {
                logger.debug("Cannot detect language: [{}]", t.getMessage());
            }
        }

        if (name != null) {
            try {
                context = context.createExternalValueContext(name);
                nameMapper.parse(context);
            } catch(MapperParsingException e){
                if (!ignoreErrors) throw e;
                if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing name: [{}]",
                        e.getMessage());
            }
        }

        if (metadata.get(Metadata.DATE) != null) {
            try {
                context = context.createExternalValueContext(metadata.get(Metadata.DATE));
                dateMapper.parse(context);
            } catch(MapperParsingException e){
                if (!ignoreErrors) throw e;
                if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing date: [{}]: [{}]",
                        e.getMessage(), context.externalValue());
            }
        }

        if (metadata.get(Metadata.TITLE) != null) {
            try {
                context = context.createExternalValueContext(metadata.get(Metadata.TITLE));
                titleMapper.parse(context);
            } catch(MapperParsingException e){
                if (!ignoreErrors) throw e;
                if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing title: [{}]: [{}]",
                        e.getMessage(), context.externalValue());
            }
        }

        if (metadata.get(Metadata.AUTHOR) != null) {
            try {
                context = context.createExternalValueContext(metadata.get(Metadata.AUTHOR));
                authorMapper.parse(context);
            } catch(MapperParsingException e){
                if (!ignoreErrors) throw e;
                if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing author: [{}]: [{}]",
                        e.getMessage(), context.externalValue());
            }
        }

        if (metadata.get(Metadata.KEYWORDS) != null) {
            try {
                context = context.createExternalValueContext(metadata.get(Metadata.KEYWORDS));
                keywordsMapper.parse(context);
            } catch(MapperParsingException e){
                if (!ignoreErrors) throw e;
                if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing keywords: [{}]: [{}]",
                        e.getMessage(), context.externalValue());
            }
        }

        if (contentType == null) {
            contentType = metadata.get(Metadata.CONTENT_TYPE);
        }
        if (contentType != null) {
            try {
                context = context.createExternalValueContext(contentType);
                contentTypeMapper.parse(context);
            } catch(MapperParsingException e){
                if (!ignoreErrors) throw e;
                if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing content_type: [{}]: [{}]", e.getMessage(), context.externalValue());
            }
        }

        int length = content.length;
        // If we have CONTENT_LENGTH from Tika we use it
        if (metadata.get(Metadata.CONTENT_LENGTH) != null) {
            length = Integer.parseInt(metadata.get(Metadata.CONTENT_LENGTH));
        }

        try {
            context = context.createExternalValueContext(length);
            contentLengthMapper.parse(context);
        } catch(MapperParsingException e){
            if (!ignoreErrors) throw e;
            if (logger.isDebugEnabled()) logger.debug("Ignoring MapperParsingException catch while parsing content_length: [{}]: [{}]", e.getMessage(), context.externalValue());
        }

//        multiFields.parse(this, context);
        if (copyTo != null) {
            copyTo.parse(context);
        }
    }

    @Override
    protected void parseCreateField(ParseContext parseContext, List<Field> fields) throws IOException {

    }

    @Override
    public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException {
        // ignore this for now
    }

    @Override
    public void traverse(FieldMapperListener fieldMapperListener) {
        contentMapper.traverse(fieldMapperListener);
        dateMapper.traverse(fieldMapperListener);
        titleMapper.traverse(fieldMapperListener);
        nameMapper.traverse(fieldMapperListener);
        authorMapper.traverse(fieldMapperListener);
        keywordsMapper.traverse(fieldMapperListener);
        contentTypeMapper.traverse(fieldMapperListener);
        contentLengthMapper.traverse(fieldMapperListener);
        languageMapper.traverse(fieldMapperListener);
    }

    @Override
    public void traverse(ObjectMapperListener objectMapperListener) {
    }

    @Override
    public void close() {
        contentMapper.close();
        dateMapper.close();
        titleMapper.close();
        nameMapper.close();
        authorMapper.close();
        keywordsMapper.close();
        contentTypeMapper.close();
        contentLengthMapper.close();
        languageMapper.close();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name());
        builder.field("type", CONTENT_TYPE);
        builder.field("path", pathType.name().toLowerCase());

        builder.startObject("fields");
        contentMapper.toXContent(builder, params);
        authorMapper.toXContent(builder, params);
        titleMapper.toXContent(builder, params);
        nameMapper.toXContent(builder, params);
        dateMapper.toXContent(builder, params);
        keywordsMapper.toXContent(builder, params);
        contentTypeMapper.toXContent(builder, params);
        contentLengthMapper.toXContent(builder, params);
        languageMapper.toXContent(builder, params);
        multiFields.toXContent(builder, params);
        builder.endObject();

        multiFields.toXContent(builder, params);
        builder.endObject();
        return builder;
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }
}
