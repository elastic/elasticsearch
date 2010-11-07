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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.lucene.search.TermFilter;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

import static org.elasticsearch.common.collect.Lists.*;

/**
 * @author kimchy (shay.banon)
 */
public class XContentDocumentMapper implements DocumentMapper, ToXContent {

    public static class Builder {

        private UidFieldMapper uidFieldMapper = new UidFieldMapper();

        private IdFieldMapper idFieldMapper = new IdFieldMapper();

        private TypeFieldMapper typeFieldMapper = new TypeFieldMapper();

        private IndexFieldMapper indexFieldMapper = new IndexFieldMapper();

        private SourceFieldMapper sourceFieldMapper = new SourceFieldMapper();

        private BoostFieldMapper boostFieldMapper = new BoostFieldMapper();

        private AllFieldMapper allFieldMapper = new AllFieldMapper();

        private AnalyzerMapper analyzerMapper = new AnalyzerMapper(null);

        private NamedAnalyzer indexAnalyzer;

        private NamedAnalyzer searchAnalyzer;

        private final String index;

        private final RootObjectMapper rootObjectMapper;

        private ImmutableMap<String, Object> attributes = ImmutableMap.of();

        private XContentMapper.BuilderContext builderContext = new XContentMapper.BuilderContext(new ContentPath(1));

        public Builder(String index, RootObjectMapper.Builder builder) {
            this.index = index;
            this.rootObjectMapper = builder.build(builderContext);
        }

        public Builder attributes(ImmutableMap<String, Object> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder sourceField(SourceFieldMapper.Builder builder) {
            this.sourceFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder idField(IdFieldMapper.Builder builder) {
            this.idFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder uidField(UidFieldMapper.Builder builder) {
            this.uidFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder typeField(TypeFieldMapper.Builder builder) {
            this.typeFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder indexField(IndexFieldMapper.Builder builder) {
            this.indexFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder boostField(BoostFieldMapper.Builder builder) {
            this.boostFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder allField(AllFieldMapper.Builder builder) {
            this.allFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder analyzerField(AnalyzerMapper.Builder builder) {
            this.analyzerMapper = builder.build(builderContext);
            return this;
        }

        public Builder indexAnalyzer(NamedAnalyzer indexAnalyzer) {
            this.indexAnalyzer = indexAnalyzer;
            return this;
        }

        public boolean hasIndexAnalyzer() {
            return indexAnalyzer != null;
        }

        public Builder searchAnalyzer(NamedAnalyzer searchAnalyzer) {
            this.searchAnalyzer = searchAnalyzer;
            return this;
        }

        public boolean hasSearchAnalyzer() {
            return searchAnalyzer != null;
        }

        public XContentDocumentMapper build(XContentDocumentMapperParser docMapperParser) {
            Preconditions.checkNotNull(rootObjectMapper, "Mapper builder must have the root object mapper set");
            return new XContentDocumentMapper(index, docMapperParser, rootObjectMapper, attributes, uidFieldMapper, idFieldMapper, typeFieldMapper, indexFieldMapper,
                    sourceFieldMapper, allFieldMapper, analyzerMapper, indexAnalyzer, searchAnalyzer, boostFieldMapper);
        }
    }


    private ThreadLocal<ThreadLocals.CleanableValue<ParseContext>> cache = new ThreadLocal<ThreadLocals.CleanableValue<ParseContext>>() {
        @Override protected ThreadLocals.CleanableValue<ParseContext> initialValue() {
            return new ThreadLocals.CleanableValue<ParseContext>(new ParseContext(index, docMapperParser, XContentDocumentMapper.this, new ContentPath(0)));
        }
    };

    private final String index;

    private final String type;

    private final XContentDocumentMapperParser docMapperParser;

    private volatile ImmutableMap<String, Object> attributes;

    private volatile CompressedString mappingSource;

    private final UidFieldMapper uidFieldMapper;

    private final IdFieldMapper idFieldMapper;

    private final TypeFieldMapper typeFieldMapper;

    private final IndexFieldMapper indexFieldMapper;

    private final SourceFieldMapper sourceFieldMapper;

    private final BoostFieldMapper boostFieldMapper;

    private final AllFieldMapper allFieldMapper;

    private final AnalyzerMapper analyzerMapper;

    private final RootObjectMapper rootObjectMapper;

    private final NamedAnalyzer indexAnalyzer;

    private final NamedAnalyzer searchAnalyzer;

    private volatile DocumentFieldMappers fieldMappers;

    private final List<FieldMapperListener> fieldMapperListeners = newArrayList();

    private final Filter typeFilter;

    private final Object mutex = new Object();

    public XContentDocumentMapper(String index, XContentDocumentMapperParser docMapperParser,
                                  RootObjectMapper rootObjectMapper,
                                  ImmutableMap<String, Object> attributes,
                                  UidFieldMapper uidFieldMapper,
                                  IdFieldMapper idFieldMapper,
                                  TypeFieldMapper typeFieldMapper,
                                  IndexFieldMapper indexFieldMapper,
                                  SourceFieldMapper sourceFieldMapper,
                                  AllFieldMapper allFieldMapper,
                                  AnalyzerMapper analyzerMapper,
                                  NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer,
                                  @Nullable BoostFieldMapper boostFieldMapper) {
        this.index = index;
        this.type = rootObjectMapper.name();
        this.docMapperParser = docMapperParser;
        this.attributes = attributes;
        this.rootObjectMapper = rootObjectMapper;
        this.uidFieldMapper = uidFieldMapper;
        this.idFieldMapper = idFieldMapper;
        this.typeFieldMapper = typeFieldMapper;
        this.indexFieldMapper = indexFieldMapper;
        this.sourceFieldMapper = sourceFieldMapper;
        this.allFieldMapper = allFieldMapper;
        this.analyzerMapper = analyzerMapper;
        this.boostFieldMapper = boostFieldMapper;

        this.indexAnalyzer = indexAnalyzer;
        this.searchAnalyzer = searchAnalyzer;

        this.typeFilter = new TermFilter(typeMapper().term(type));

        // if we are not enabling all, set it to false on the root object, (and on all the rest...)
        if (!allFieldMapper.enabled()) {
            this.rootObjectMapper.includeInAll(allFieldMapper.enabled());
        }

        rootObjectMapper.putMapper(idFieldMapper);
        if (boostFieldMapper != null) {
            rootObjectMapper.putMapper(boostFieldMapper);
        }

        final List<FieldMapper> tempFieldMappers = newArrayList();
        // add the basic ones
        if (indexFieldMapper.enabled()) {
            tempFieldMappers.add(indexFieldMapper);
        }
        tempFieldMappers.add(typeFieldMapper);
        tempFieldMappers.add(sourceFieldMapper);
        tempFieldMappers.add(uidFieldMapper);
        tempFieldMappers.add(allFieldMapper);
        if (boostFieldMapper != null) {
            tempFieldMappers.add(boostFieldMapper);
        }
        // now traverse and get all the statically defined ones
        rootObjectMapper.traverse(new FieldMapperListener() {
            @Override public void fieldMapper(FieldMapper fieldMapper) {
                tempFieldMappers.add(fieldMapper);
            }
        });

        this.fieldMappers = new DocumentFieldMappers(this, tempFieldMappers);

        refreshSource();
    }

    @Override public String type() {
        return this.type;
    }

    @Override public ImmutableMap<String, Object> attributes() {
        return this.attributes;
    }

    @Override public CompressedString mappingSource() {
        return this.mappingSource;
    }

    public RootObjectMapper root() {
        return this.rootObjectMapper;
    }

    @Override public org.elasticsearch.index.mapper.UidFieldMapper uidMapper() {
        return this.uidFieldMapper;
    }

    @Override public org.elasticsearch.index.mapper.IdFieldMapper idMapper() {
        return this.idFieldMapper;
    }

    @Override public org.elasticsearch.index.mapper.IndexFieldMapper indexMapper() {
        return this.indexFieldMapper;
    }

    @Override public org.elasticsearch.index.mapper.TypeFieldMapper typeMapper() {
        return this.typeFieldMapper;
    }

    @Override public org.elasticsearch.index.mapper.SourceFieldMapper sourceMapper() {
        return this.sourceFieldMapper;
    }

    @Override public org.elasticsearch.index.mapper.BoostFieldMapper boostMapper() {
        return this.boostFieldMapper;
    }

    @Override public org.elasticsearch.index.mapper.AllFieldMapper allFieldMapper() {
        return this.allFieldMapper;
    }

    @Override public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    @Override public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    @Override public Filter typeFilter() {
        return this.typeFilter;
    }

    @Override public DocumentFieldMappers mappers() {
        return this.fieldMappers;
    }

    @Override public ParsedDocument parse(byte[] source) {
        return parse(null, null, source);
    }

    @Override public ParsedDocument parse(@Nullable String type, @Nullable String id, byte[] source) throws MapperParsingException {
        return parse(type, id, source, ParseListener.EMPTY);
    }

    @Override public ParsedDocument parse(String type, String id, byte[] source, ParseListener listener) {
        ParseContext context = cache.get().get();

        if (type != null && !type.equals(this.type)) {
            throw new MapperParsingException("Type mismatch, provide type [" + type + "] but mapper is of type [" + this.type + "]");
        }
        type = this.type;

        XContentParser parser = null;
        try {
            parser = XContentFactory.xContent(source).createParser(source);
            context.reset(parser, new Document(), type, source, listener);

            // will result in START_OBJECT
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new MapperException("Malformed content, must start with an object");
            }
            token = parser.nextToken();
            if (token != XContentParser.Token.FIELD_NAME) {
                throw new MapperException("Malformed content, after first object, either the type field or the actual properties should exist");
            }
            if (parser.currentName().equals(type)) {
                // first field is the same as the type, this might be because the type is provided, and the object exists within it
                // or because there is a valid field that by chance is named as the type

                // Note, in this case, we only handle plain value types, an object type will be analyzed as if it was the type itself
                // and other same level fields will be ignored
                token = parser.nextToken();
                // commented out, allow for same type with START_OBJECT, we do our best to handle it except for the above corner case
//                if (token != XContentParser.Token.START_OBJECT) {
//                    throw new MapperException("Malformed content, a field with the same name as the type must be an object with the properties/fields within it");
//                }
            }

            if (sourceFieldMapper.enabled()) {
                sourceFieldMapper.parse(context);
            }
            // set the id if we have it so we can validate it later on, also, add the uid if we can
            if (id != null) {
                context.id(id);
                uidFieldMapper.parse(context);
            }
            typeFieldMapper.parse(context);

            indexFieldMapper.parse(context);

            rootObjectMapper.parse(context);

            // if we did not get the id, we need to parse the uid into the document now, after it was added
            if (id == null) {
                uidFieldMapper.parse(context);
            }
            if (context.parsedIdState() != ParseContext.ParsedIdState.PARSED) {
                // mark it as external, so we can parse it
                context.parsedId(ParseContext.ParsedIdState.EXTERNAL);
                idFieldMapper.parse(context);
            }
            analyzerMapper.parse(context);
            allFieldMapper.parse(context);
        } catch (IOException e) {
            throw new MapperParsingException("Failed to parse", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
        return new ParsedDocument(context.uid(), context.id(), context.type(), context.doc(), context.analyzer(), source, context.mappersAdded());
    }

    void addFieldMapper(FieldMapper fieldMapper) {
        synchronized (mutex) {
            fieldMappers = fieldMappers.concat(this, fieldMapper);
            for (FieldMapperListener listener : fieldMapperListeners) {
                listener.fieldMapper(fieldMapper);
            }
        }
    }

    @Override public void addFieldMapperListener(FieldMapperListener fieldMapperListener, boolean includeExisting) {
        synchronized (mutex) {
            fieldMapperListeners.add(fieldMapperListener);
            if (includeExisting) {
                if (indexFieldMapper.enabled()) {
                    fieldMapperListener.fieldMapper(indexFieldMapper);
                }
                fieldMapperListener.fieldMapper(sourceFieldMapper);
                fieldMapperListener.fieldMapper(typeFieldMapper);
                fieldMapperListener.fieldMapper(idFieldMapper);
                fieldMapperListener.fieldMapper(uidFieldMapper);
                fieldMapperListener.fieldMapper(allFieldMapper);
                rootObjectMapper.traverse(fieldMapperListener);
            }
        }
    }

    @Override public synchronized MergeResult merge(DocumentMapper mergeWith, MergeFlags mergeFlags) {
        XContentDocumentMapper xContentMergeWith = (XContentDocumentMapper) mergeWith;
        MergeContext mergeContext = new MergeContext(this, mergeFlags);
        rootObjectMapper.merge(xContentMergeWith.rootObjectMapper, mergeContext);
        if (!mergeFlags.simulate()) {
            // let the merge with attributes to override the attributes
            attributes = mergeWith.attributes();
            // update the source of the merged one
            refreshSource();
        }
        return new MergeResult(mergeContext.buildConflicts());
    }

    @Override public void refreshSource() throws FailedToGenerateSourceMapperException {
        try {
            XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
            builder.startObject();
            toXContent(builder, ToXContent.EMPTY_PARAMS);
            builder.endObject();
            this.mappingSource = new CompressedString(builder.string());
        } catch (Exception e) {
            throw new FailedToGenerateSourceMapperException(e.getMessage(), e);
        }
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        rootObjectMapper.toXContent(builder, params, new ToXContent() {
            @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
                if (indexAnalyzer != null && searchAnalyzer != null && indexAnalyzer.name().equals(searchAnalyzer.name()) && !indexAnalyzer.name().startsWith("_")) {
                    if (!indexAnalyzer.name().equals("default")) {
                        // same analyzers, output it once
                        builder.field("analyzer", indexAnalyzer.name());
                    }
                } else {
                    if (indexAnalyzer != null && !indexAnalyzer.name().startsWith("_")) {
                        if (!indexAnalyzer.name().equals("default")) {
                            builder.field("index_analyzer", indexAnalyzer.name());
                        }
                    }
                    if (searchAnalyzer != null && !searchAnalyzer.name().startsWith("_")) {
                        if (!searchAnalyzer.name().equals("default")) {
                            builder.field("search_analyzer", searchAnalyzer.name());
                        }
                    }
                }

                if (attributes != null && !attributes.isEmpty()) {
                    builder.field("_attributes", attributes());
                }
            }
            // no need to pass here id and boost, since they are added to the root object mapper
            // in the constructor
        }, indexFieldMapper, typeFieldMapper, allFieldMapper, analyzerMapper, sourceFieldMapper);
    }
}
