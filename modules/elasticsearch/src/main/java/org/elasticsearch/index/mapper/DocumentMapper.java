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

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.compress.lzf.LZF;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.CachedStreamInput;
import org.elasticsearch.common.io.stream.LZFStreamInput;
import org.elasticsearch.common.lucene.uid.UidField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.internal.*;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.collect.Lists.*;

/**
 * @author kimchy (shay.banon)
 */
public class DocumentMapper implements ToXContent {

    /**
     * A result of a merge.
     */
    public static class MergeResult {

        private final String[] conflicts;

        public MergeResult(String[] conflicts) {
            this.conflicts = conflicts;
        }

        /**
         * Does the merge have conflicts or not?
         */
        public boolean hasConflicts() {
            return conflicts.length > 0;
        }

        /**
         * The merge conflicts.
         */
        public String[] conflicts() {
            return this.conflicts;
        }
    }

    public static class MergeFlags {

        public static MergeFlags mergeFlags() {
            return new MergeFlags();
        }

        private boolean simulate = true;

        public MergeFlags() {
        }

        /**
         * A simulation run, don't perform actual modifications to the mapping.
         */
        public boolean simulate() {
            return simulate;
        }

        public MergeFlags simulate(boolean simulate) {
            this.simulate = simulate;
            return this;
        }
    }

    /**
     * A listener to be called during the parse process.
     */
    public static interface ParseListener<ParseContext> {

        public static final ParseListener EMPTY = new ParseListenerAdapter();

        /**
         * Called before a field is added to the document. Return <tt>true</tt> to include
         * it in the document.
         */
        boolean beforeFieldAdded(FieldMapper fieldMapper, Fieldable fieldable, ParseContext parseContent);
    }

    public static class ParseListenerAdapter implements ParseListener {

        @Override public boolean beforeFieldAdded(FieldMapper fieldMapper, Fieldable fieldable, Object parseContext) {
            return true;
        }
    }

    public static class Builder {

        private UidFieldMapper uidFieldMapper = new UidFieldMapper();

        private IdFieldMapper idFieldMapper = new IdFieldMapper();

        private TypeFieldMapper typeFieldMapper = new TypeFieldMapper();

        private IndexFieldMapper indexFieldMapper = new IndexFieldMapper();

        private SourceFieldMapper sourceFieldMapper = new SourceFieldMapper();
        private SizeFieldMapper sizeFieldMapper = new SizeFieldMapper();

        private RoutingFieldMapper routingFieldMapper = new RoutingFieldMapper();

        private BoostFieldMapper boostFieldMapper = new BoostFieldMapper();

        private AllFieldMapper allFieldMapper = new AllFieldMapper();

        private AnalyzerMapper analyzerMapper = new AnalyzerMapper();

        private ParentFieldMapper parentFieldMapper = null;

        private NamedAnalyzer indexAnalyzer;

        private NamedAnalyzer searchAnalyzer;

        private final String index;

        private final RootObjectMapper rootObjectMapper;

        private ImmutableMap<String, Object> meta = ImmutableMap.of();

        private Mapper.BuilderContext builderContext = new Mapper.BuilderContext(new ContentPath(1));

        public Builder(String index, @Nullable Settings indexSettings, RootObjectMapper.Builder builder) {
            this.index = index;
            this.rootObjectMapper = builder.build(builderContext);
            if (indexSettings != null) {
                String idIndexed = indexSettings.get("index.mapping._id.indexed");
                if (idIndexed != null && Booleans.parseBoolean(idIndexed, false)) {
                    idFieldMapper = new IdFieldMapper(Field.Index.NOT_ANALYZED);
                }
            }
        }

        public Builder meta(ImmutableMap<String, Object> meta) {
            this.meta = meta;
            return this;
        }

        public Builder sourceField(SourceFieldMapper.Builder builder) {
            this.sourceFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder sizeField(SizeFieldMapper.Builder builder) {
            this.sizeFieldMapper = builder.build(builderContext);
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

        public Builder routingField(RoutingFieldMapper.Builder builder) {
            this.routingFieldMapper = builder.build(builderContext);
            return this;
        }

        public Builder parentFiled(ParentFieldMapper.Builder builder) {
            this.parentFieldMapper = builder.build(builderContext);
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

        public DocumentMapper build(DocumentMapperParser docMapperParser) {
            Preconditions.checkNotNull(rootObjectMapper, "Mapper builder must have the root object mapper set");
            return new DocumentMapper(index, docMapperParser, rootObjectMapper, meta, uidFieldMapper, idFieldMapper, typeFieldMapper, indexFieldMapper,
                    sourceFieldMapper, sizeFieldMapper, parentFieldMapper, routingFieldMapper, allFieldMapper, analyzerMapper, indexAnalyzer, searchAnalyzer, boostFieldMapper);
        }
    }


    private ThreadLocal<ParseContext> cache = new ThreadLocal<ParseContext>() {
        @Override protected ParseContext initialValue() {
            return new ParseContext(index, docMapperParser, DocumentMapper.this, new ContentPath(0));
        }
    };

    private final String index;

    private final String type;

    private final DocumentMapperParser docMapperParser;

    private volatile ImmutableMap<String, Object> meta;

    private volatile CompressedString mappingSource;

    private final UidFieldMapper uidFieldMapper;

    private final IdFieldMapper idFieldMapper;

    private final TypeFieldMapper typeFieldMapper;

    private final IndexFieldMapper indexFieldMapper;

    private final SourceFieldMapper sourceFieldMapper;
    private final SizeFieldMapper sizeFieldMapper;

    private final RoutingFieldMapper routingFieldMapper;

    private final ParentFieldMapper parentFieldMapper;

    private final BoostFieldMapper boostFieldMapper;

    private final AllFieldMapper allFieldMapper;

    private final AnalyzerMapper analyzerMapper;

    private final RootObjectMapper rootObjectMapper;

    private final NamedAnalyzer indexAnalyzer;

    private final NamedAnalyzer searchAnalyzer;

    private volatile DocumentFieldMappers fieldMappers;

    private volatile ImmutableMap<String, ObjectMapper> objectMappers = ImmutableMap.of();

    private final List<FieldMapperListener> fieldMapperListeners = newArrayList();

    private final List<ObjectMapperListener> objectMapperListeners = newArrayList();

    private boolean hasNestedObjects = false;

    private final Filter typeFilter;

    private final Object mutex = new Object();

    public DocumentMapper(String index, DocumentMapperParser docMapperParser,
                          RootObjectMapper rootObjectMapper,
                          ImmutableMap<String, Object> meta,
                          UidFieldMapper uidFieldMapper,
                          IdFieldMapper idFieldMapper,
                          TypeFieldMapper typeFieldMapper,
                          IndexFieldMapper indexFieldMapper,
                          SourceFieldMapper sourceFieldMapper,
                          SizeFieldMapper sizeFieldMapper,
                          @Nullable ParentFieldMapper parentFieldMapper,
                          RoutingFieldMapper routingFieldMapper,
                          AllFieldMapper allFieldMapper,
                          AnalyzerMapper analyzerMapper,
                          NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer,
                          @Nullable BoostFieldMapper boostFieldMapper) {
        this.index = index;
        this.type = rootObjectMapper.name();
        this.docMapperParser = docMapperParser;
        this.meta = meta;
        this.rootObjectMapper = rootObjectMapper;
        this.uidFieldMapper = uidFieldMapper;
        this.idFieldMapper = idFieldMapper;
        this.typeFieldMapper = typeFieldMapper;
        this.indexFieldMapper = indexFieldMapper;
        this.sourceFieldMapper = sourceFieldMapper;
        this.sizeFieldMapper = sizeFieldMapper;
        this.parentFieldMapper = parentFieldMapper;
        this.routingFieldMapper = routingFieldMapper;
        this.allFieldMapper = allFieldMapper;
        this.analyzerMapper = analyzerMapper;
        this.boostFieldMapper = boostFieldMapper;

        this.indexAnalyzer = indexAnalyzer;
        this.searchAnalyzer = searchAnalyzer;

        this.typeFilter = typeMapper().fieldFilter(type);

        rootObjectMapper.putMapper(idFieldMapper);
        if (boostFieldMapper != null) {
            rootObjectMapper.putMapper(boostFieldMapper);
        }
        if (parentFieldMapper != null) {
            rootObjectMapper.putMapper(parentFieldMapper);
            // also, mark the routing as required!
            routingFieldMapper.markAsRequired();
        }
        rootObjectMapper.putMapper(routingFieldMapper);

        final List<FieldMapper> tempFieldMappers = newArrayList();
        // add the basic ones
        if (indexFieldMapper.enabled()) {
            tempFieldMappers.add(indexFieldMapper);
        }
        tempFieldMappers.add(typeFieldMapper);
        tempFieldMappers.add(sourceFieldMapper);
        tempFieldMappers.add(sizeFieldMapper);
        tempFieldMappers.add(uidFieldMapper);
        tempFieldMappers.add(allFieldMapper);
        // now traverse and get all the statically defined ones
        rootObjectMapper.traverse(new FieldMapperListener() {
            @Override public void fieldMapper(FieldMapper fieldMapper) {
                tempFieldMappers.add(fieldMapper);
            }
        });

        this.fieldMappers = new DocumentFieldMappers(this, tempFieldMappers);

        final Map<String, ObjectMapper> objectMappers = Maps.newHashMap();
        rootObjectMapper.traverse(new ObjectMapperListener() {
            @Override public void objectMapper(ObjectMapper objectMapper) {
                objectMappers.put(objectMapper.fullPath(), objectMapper);
            }
        });
        this.objectMappers = ImmutableMap.copyOf(objectMappers);
        for (ObjectMapper objectMapper : objectMappers.values()) {
            if (objectMapper.nested().isNested()) {
                hasNestedObjects = true;
            }
        }

        refreshSource();
    }

    public String type() {
        return this.type;
    }

    public ImmutableMap<String, Object> meta() {
        return this.meta;
    }

    public CompressedString mappingSource() {
        return this.mappingSource;
    }

    public RootObjectMapper root() {
        return this.rootObjectMapper;
    }

    public UidFieldMapper uidMapper() {
        return this.uidFieldMapper;
    }

    public IdFieldMapper idMapper() {
        return this.idFieldMapper;
    }

    public IndexFieldMapper indexMapper() {
        return this.indexFieldMapper;
    }

    public TypeFieldMapper typeMapper() {
        return this.typeFieldMapper;
    }

    public SourceFieldMapper sourceMapper() {
        return this.sourceFieldMapper;
    }

    public BoostFieldMapper boostMapper() {
        return this.boostFieldMapper;
    }

    public AllFieldMapper allFieldMapper() {
        return this.allFieldMapper;
    }

    public RoutingFieldMapper routingFieldMapper() {
        return this.routingFieldMapper;
    }

    public ParentFieldMapper parentFieldMapper() {
        return this.parentFieldMapper;
    }

    public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    public Filter typeFilter() {
        return this.typeFilter;
    }

    public boolean hasNestedObjects() {
        return hasNestedObjects;
    }

    public DocumentFieldMappers mappers() {
        return this.fieldMappers;
    }

    public ImmutableMap<String, ObjectMapper> objectMappers() {
        return this.objectMappers;
    }

    public ParsedDocument parse(byte[] source) throws MapperParsingException {
        return parse(SourceToParse.source(source));
    }

    public ParsedDocument parse(String type, String id, byte[] source) throws MapperParsingException {
        return parse(SourceToParse.source(source).type(type).id(id));
    }

    public ParsedDocument parse(SourceToParse source) throws MapperParsingException {
        return parse(source, null);
    }

    public ParsedDocument parse(SourceToParse source, @Nullable ParseListener listener) throws MapperParsingException {
        ParseContext context = cache.get();

        if (source.type() != null && !source.type().equals(this.type)) {
            throw new MapperParsingException("Type mismatch, provide type [" + source.type() + "] but mapper is of type [" + this.type + "]");
        }
        source.type(this.type);

        XContentParser parser = source.parser();
        try {
            if (parser == null) {
                if (LZF.isCompressed(source.source())) {
                    BytesStreamInput siBytes = new BytesStreamInput(source.source());
                    LZFStreamInput siLzf = CachedStreamInput.cachedLzf(siBytes);
                    XContentType contentType = XContentFactory.xContentType(siLzf);
                    siLzf.resetToBufferStart();
                    parser = XContentFactory.xContent(contentType).createParser(siLzf);
                } else {
                    parser = XContentFactory.xContent(source.source()).createParser(source.source());
                }
            }
            context.reset(parser, new Document(), type, source.source(), source.flyweight(), listener);

            // will result in START_OBJECT
            int countDownTokens = 0;
            XContentParser.Token token = parser.nextToken();
            if (token != XContentParser.Token.START_OBJECT) {
                throw new MapperParsingException("Malformed content, must start with an object");
            }
            boolean emptyDoc = false;
            token = parser.nextToken();
            if (token == XContentParser.Token.END_OBJECT) {
                // empty doc, we can handle it...
                emptyDoc = true;
            } else if (token != XContentParser.Token.FIELD_NAME) {
                throw new MapperParsingException("Malformed content, after first object, either the type field or the actual properties should exist");
            }
            if (type.equals(parser.currentName())) {
                // first field is the same as the type, this might be because the type is provided, and the object exists within it
                // or because there is a valid field that by chance is named as the type

                // Note, in this case, we only handle plain value types, an object type will be analyzed as if it was the type itself
                // and other same level fields will be ignored
                token = parser.nextToken();
                countDownTokens++;
                // commented out, allow for same type with START_OBJECT, we do our best to handle it except for the above corner case
//                if (token != XContentParser.Token.START_OBJECT) {
//                    throw new MapperException("Malformed content, a field with the same name as the type must be an object with the properties/fields within it");
//                }
            }

            if (sizeFieldMapper.enabled()) {
                context.externalValue(source.source().length);
                sizeFieldMapper.parse(context);
            }

            if (sourceFieldMapper.enabled()) {
                sourceFieldMapper.parse(context);
            }
            // set the id if we have it so we can validate it later on, also, add the uid if we can
            if (source.id() != null) {
                context.id(source.id());
                uidFieldMapper.parse(context);
            }
            typeFieldMapper.parse(context);
            if (source.routing() != null) {
                context.externalValue(source.routing());
                routingFieldMapper.parse(context);
            }

            indexFieldMapper.parse(context);

            if (!emptyDoc) {
                rootObjectMapper.parse(context);
            }

            for (int i = 0; i < countDownTokens; i++) {
                parser.nextToken();
            }

            // if we did not get the id, we need to parse the uid into the document now, after it was added
            if (source.id() == null) {
                if (context.id() == null) {
                    if (!source.flyweight()) {
                        throw new MapperParsingException("No id found while parsing the content source");
                    }
                } else {
                    uidFieldMapper.parse(context);
                    if (context.docs().size() > 1) {
                        UidField uidField = (UidField) context.doc().getFieldable(UidFieldMapper.NAME);
                        assert uidField != null;
                        // we need to go over the docs and add it...
                        for (int i = 1; i < context.docs().size(); i++) {
                            // we don't need to add it as a full uid field in nested docs, since we don't need versioning
                            context.docs().get(i).add(new Field(UidFieldMapper.NAME, uidField.uid(), Field.Store.NO, Field.Index.NOT_ANALYZED));
                        }
                    }
                }
            }
            if (context.parsedIdState() != ParseContext.ParsedIdState.PARSED) {
                if (context.id() == null) {
                    if (!source.flyweight()) {
                        throw new MapperParsingException("No id mapping with [_id] found in the content, and not explicitly set");
                    }
                } else {
                    // mark it as external, so we can parse it
                    context.parsedId(ParseContext.ParsedIdState.EXTERNAL);
                    idFieldMapper.parse(context);
                }
            }
            if (parentFieldMapper != null) {
                context.externalValue(source.parent());
                parentFieldMapper.parse(context);
            }
            analyzerMapper.parse(context);
            allFieldMapper.parse(context);
            // validate aggregated mappers (TODO: need to be added as a phase to any field mapper)
            routingFieldMapper.validate(context, source.routing());
        } catch (IOException e) {
            throw new MapperParsingException("Failed to parse", e);
        } finally {
            // only close the parser when its not provided externally
            if (source.parser() == null && parser != null) {
                parser.close();
            }
        }
        // reverse the order of docs for nested docs support, parent should be last
        if (context.docs().size() > 1) {
            Collections.reverse(context.docs());
        }
        ParsedDocument doc = new ParsedDocument(context.uid(), context.id(), context.type(), source.routing(), context.docs(), context.analyzer(),
                context.source(), context.mappersAdded()).parent(source.parent());
        // reset the context to free up memory
        context.reset(null, null, null, null, false, null);
        return doc;
    }

    public void addFieldMapper(FieldMapper fieldMapper) {
        synchronized (mutex) {
            fieldMappers = fieldMappers.concat(this, fieldMapper);
            for (FieldMapperListener listener : fieldMapperListeners) {
                listener.fieldMapper(fieldMapper);
            }
        }
    }

    public void addFieldMapperListener(FieldMapperListener fieldMapperListener, boolean includeExisting) {
        synchronized (mutex) {
            fieldMapperListeners.add(fieldMapperListener);
            if (includeExisting) {
                if (indexFieldMapper.enabled()) {
                    fieldMapperListener.fieldMapper(indexFieldMapper);
                }
                fieldMapperListener.fieldMapper(sourceFieldMapper);
                fieldMapperListener.fieldMapper(sizeFieldMapper);
                fieldMapperListener.fieldMapper(typeFieldMapper);
                fieldMapperListener.fieldMapper(uidFieldMapper);
                fieldMapperListener.fieldMapper(allFieldMapper);
                rootObjectMapper.traverse(fieldMapperListener);
            }
        }
    }

    public void addObjectMapper(ObjectMapper objectMapper) {
        synchronized (mutex) {
            objectMappers = MapBuilder.newMapBuilder(objectMappers).put(objectMapper.fullPath(), objectMapper).immutableMap();
            for (ObjectMapperListener objectMapperListener : objectMapperListeners) {
                objectMapperListener.objectMapper(objectMapper);
            }
            if (objectMapper.nested().isNested()) {
                hasNestedObjects = true;
            }
        }
    }

    public void addObjectMapperListener(ObjectMapperListener objectMapperListener, boolean includeExisting) {
        synchronized (mutex) {
            objectMapperListeners.add(objectMapperListener);
            if (includeExisting) {
                rootObjectMapper.traverse(objectMapperListener);
            }
        }
    }

    public synchronized MergeResult merge(DocumentMapper mergeWith, MergeFlags mergeFlags) {
        MergeContext mergeContext = new MergeContext(this, mergeFlags);
        rootObjectMapper.merge(mergeWith.rootObjectMapper, mergeContext);

        allFieldMapper.merge(mergeWith.allFieldMapper, mergeContext);
        analyzerMapper.merge(mergeWith.analyzerMapper, mergeContext);
        sourceFieldMapper.merge(mergeWith.sourceFieldMapper, mergeContext);
        sizeFieldMapper.merge(mergeWith.sizeFieldMapper, mergeContext);

        if (!mergeFlags.simulate()) {
            // let the merge with attributes to override the attributes
            meta = mergeWith.meta();
            // update the source of the merged one
            refreshSource();
        }
        return new MergeResult(mergeContext.buildConflicts());
    }

    public void refreshSource() throws FailedToGenerateSourceMapperException {
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

    public void close() {
        cache.remove();
        rootObjectMapper.close();
        idFieldMapper.close();
        indexFieldMapper.close();
        typeFieldMapper.close();
        allFieldMapper.close();
        analyzerMapper.close();
        sourceFieldMapper.close();
        sizeFieldMapper.close();
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        rootObjectMapper.toXContent(builder, params, new ToXContent() {
            @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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

                if (meta != null && !meta.isEmpty()) {
                    builder.field("_meta", meta());
                }
                return builder;
            }
            // no need to pass here id and boost, since they are added to the root object mapper
            // in the constructor
        }, indexFieldMapper, typeFieldMapper, allFieldMapper, analyzerMapper, sourceFieldMapper, sizeFieldMapper);
        return builder;
    }
}
