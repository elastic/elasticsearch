/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.lucene.search.Filter;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.compress.CompressedString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.*;
import org.elasticsearch.index.analysis.NamedAnalyzer;
import org.elasticsearch.index.mapper.internal.*;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.google.common.collect.Lists.newArrayList;

/**
 *
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

        @Override
        public boolean beforeFieldAdded(FieldMapper fieldMapper, Fieldable fieldable, Object parseContext) {
            return true;
        }
    }

    public static class Builder {

        private Map<Class<? extends RootMapper>, RootMapper> rootMappers = new LinkedHashMap<Class<? extends RootMapper>, RootMapper>();

        private NamedAnalyzer indexAnalyzer;

        private NamedAnalyzer searchAnalyzer;

        private NamedAnalyzer searchQuoteAnalyzer;

        private final String index;

        @Nullable
        private final Settings indexSettings;

        private final RootObjectMapper rootObjectMapper;

        private ImmutableMap<String, Object> meta = ImmutableMap.of();

        private final Mapper.BuilderContext builderContext;

        public Builder(String index, @Nullable Settings indexSettings, RootObjectMapper.Builder builder) {
            this.index = index;
            this.indexSettings = indexSettings;
            this.builderContext = new Mapper.BuilderContext(indexSettings, new ContentPath(1));
            this.rootObjectMapper = builder.build(builderContext);
            IdFieldMapper idFieldMapper = new IdFieldMapper();
            if (indexSettings != null) {
                String idIndexed = indexSettings.get("index.mapping._id.indexed");
                if (idIndexed != null && Booleans.parseBoolean(idIndexed, false)) {
                    idFieldMapper = new IdFieldMapper(Field.Index.NOT_ANALYZED);
                }
            }
            this.rootMappers.put(IdFieldMapper.class, idFieldMapper);
            // add default mappers, order is important (for example analyzer should come before the rest to set context.analyzer)
            this.rootMappers.put(SizeFieldMapper.class, new SizeFieldMapper());
            this.rootMappers.put(IndexFieldMapper.class, new IndexFieldMapper());
            this.rootMappers.put(SourceFieldMapper.class, new SourceFieldMapper());
            this.rootMappers.put(TypeFieldMapper.class, new TypeFieldMapper());
            this.rootMappers.put(AnalyzerMapper.class, new AnalyzerMapper());
            this.rootMappers.put(AllFieldMapper.class, new AllFieldMapper());
            this.rootMappers.put(BoostFieldMapper.class, new BoostFieldMapper());
            this.rootMappers.put(RoutingFieldMapper.class, new RoutingFieldMapper());
            this.rootMappers.put(TimestampFieldMapper.class, new TimestampFieldMapper());
            this.rootMappers.put(TTLFieldMapper.class, new TTLFieldMapper());
            this.rootMappers.put(UidFieldMapper.class, new UidFieldMapper());
            // don't add parent field, by default its "null"
        }

        public Builder meta(ImmutableMap<String, Object> meta) {
            this.meta = meta;
            return this;
        }

        public Builder put(RootMapper.Builder mapper) {
            RootMapper rootMapper = (RootMapper) mapper.build(builderContext);
            rootMappers.put(rootMapper.getClass(), rootMapper);
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
            if (this.searchQuoteAnalyzer == null) {
                this.searchQuoteAnalyzer = searchAnalyzer;
            }
            return this;
        }

        public Builder searchQuoteAnalyzer(NamedAnalyzer searchQuoteAnalyzer) {
            this.searchQuoteAnalyzer = searchQuoteAnalyzer;
            return this;
        }

        public boolean hasSearchAnalyzer() {
            return searchAnalyzer != null;
        }

        public boolean hasSearchQuoteAnalyzer() {
            return searchQuoteAnalyzer != null;
        }

        public DocumentMapper build(DocumentMapperParser docMapperParser) {
            Preconditions.checkNotNull(rootObjectMapper, "Mapper builder must have the root object mapper set");
            return new DocumentMapper(index, indexSettings, docMapperParser, rootObjectMapper, meta,
                    indexAnalyzer, searchAnalyzer, searchQuoteAnalyzer,
                    rootMappers);
        }
    }


    private ThreadLocal<ParseContext> cache = new ThreadLocal<ParseContext>() {
        @Override
        protected ParseContext initialValue() {
            return new ParseContext(index, indexSettings, docMapperParser, DocumentMapper.this, new ContentPath(0));
        }
    };

    private final String index;

    private final Settings indexSettings;

    private final String type;

    private final DocumentMapperParser docMapperParser;

    private volatile ImmutableMap<String, Object> meta;

    private volatile CompressedString mappingSource;

    private final RootObjectMapper rootObjectMapper;

    private final ImmutableMap<Class<? extends RootMapper>, RootMapper> rootMappers;
    private final RootMapper[] rootMappersOrdered;
    private final RootMapper[] rootMappersNotIncludedInObject;

    private final NamedAnalyzer indexAnalyzer;

    private final NamedAnalyzer searchAnalyzer;
    private final NamedAnalyzer searchQuoteAnalyzer;

    private volatile DocumentFieldMappers fieldMappers;

    private volatile ImmutableMap<String, ObjectMapper> objectMappers = ImmutableMap.of();

    private final List<FieldMapperListener> fieldMapperListeners = new CopyOnWriteArrayList<FieldMapperListener>();

    private final List<ObjectMapperListener> objectMapperListeners = new CopyOnWriteArrayList<ObjectMapperListener>();

    private boolean hasNestedObjects = false;

    private final Filter typeFilter;

    private final Object mutex = new Object();

    private boolean initMappersAdded = true;

    public DocumentMapper(String index, @Nullable Settings indexSettings, DocumentMapperParser docMapperParser,
                          RootObjectMapper rootObjectMapper,
                          ImmutableMap<String, Object> meta,
                          NamedAnalyzer indexAnalyzer, NamedAnalyzer searchAnalyzer, NamedAnalyzer searchQuoteAnalyzer,
                          Map<Class<? extends RootMapper>, RootMapper> rootMappers) {
        this.index = index;
        this.indexSettings = indexSettings;
        this.type = rootObjectMapper.name();
        this.docMapperParser = docMapperParser;
        this.meta = meta;
        this.rootObjectMapper = rootObjectMapper;

        this.rootMappers = ImmutableMap.copyOf(rootMappers);
        this.rootMappersOrdered = rootMappers.values().toArray(new RootMapper[rootMappers.values().size()]);
        List<RootMapper> rootMappersNotIncludedInObjectLst = newArrayList();
        for (RootMapper rootMapper : rootMappersOrdered) {
            if (!rootMapper.includeInObject()) {
                rootMappersNotIncludedInObjectLst.add(rootMapper);
            }
        }
        this.rootMappersNotIncludedInObject = rootMappersNotIncludedInObjectLst.toArray(new RootMapper[rootMappersNotIncludedInObjectLst.size()]);

        this.indexAnalyzer = indexAnalyzer;
        this.searchAnalyzer = searchAnalyzer;
        this.searchQuoteAnalyzer = searchQuoteAnalyzer != null ? searchQuoteAnalyzer : searchAnalyzer;

        this.typeFilter = typeMapper().fieldFilter(type, null);

        if (rootMapper(ParentFieldMapper.class) != null) {
            // mark the routing field mapper as required
            rootMapper(RoutingFieldMapper.class).markAsRequired();
        }

        final List<FieldMapper> tempFieldMappers = newArrayList();
        for (RootMapper rootMapper : rootMappersOrdered) {
            if (rootMapper.includeInObject()) {
                rootObjectMapper.putMapper(rootMapper);
            } else {
                if (rootMapper instanceof FieldMapper) {
                    tempFieldMappers.add((FieldMapper) rootMapper);
                }
            }
        }

        // now traverse and get all the statically defined ones
        rootObjectMapper.traverse(new FieldMapperListener() {
            @Override
            public void fieldMapper(FieldMapper fieldMapper) {
                tempFieldMappers.add(fieldMapper);
            }
        });

        this.fieldMappers = new DocumentFieldMappers(this, tempFieldMappers);

        final Map<String, ObjectMapper> objectMappers = Maps.newHashMap();
        rootObjectMapper.traverse(new ObjectMapperListener() {
            @Override
            public void objectMapper(ObjectMapper objectMapper) {
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
        return rootMapper(UidFieldMapper.class);
    }

    @SuppressWarnings({"unchecked"})
    public <T extends RootMapper> T rootMapper(Class<T> type) {
        return (T) rootMappers.get(type);
    }

    public TypeFieldMapper typeMapper() {
        return rootMapper(TypeFieldMapper.class);
    }

    public SourceFieldMapper sourceMapper() {
        return rootMapper(SourceFieldMapper.class);
    }

    public AllFieldMapper allFieldMapper() {
        return rootMapper(AllFieldMapper.class);
    }

    public IdFieldMapper idFieldMapper() {
        return rootMapper(IdFieldMapper.class);
    }

    public RoutingFieldMapper routingFieldMapper() {
        return rootMapper(RoutingFieldMapper.class);
    }

    public ParentFieldMapper parentFieldMapper() {
        return rootMapper(ParentFieldMapper.class);
    }

    public TimestampFieldMapper timestampFieldMapper() {
        return rootMapper(TimestampFieldMapper.class);
    }

    public TTLFieldMapper TTLFieldMapper() {
        return rootMapper(TTLFieldMapper.class);
    }

    public Analyzer indexAnalyzer() {
        return this.indexAnalyzer;
    }

    public Analyzer searchAnalyzer() {
        return this.searchAnalyzer;
    }

    public Analyzer searchQuotedAnalyzer() {
        return this.searchQuoteAnalyzer;
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

    public ParsedDocument parse(BytesReference source) throws MapperParsingException {
        return parse(SourceToParse.source(source));
    }

    public ParsedDocument parse(String type, String id, BytesReference source) throws MapperParsingException {
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
                parser = XContentHelper.createParser(source.source());
            }
            context.reset(parser, new Document(), source, listener);
            // on a newly created instance of document mapper, we always consider it as new mappers that have been added
            if (initMappersAdded) {
                context.addedMapper();
                initMappersAdded = false;
            }

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

            for (RootMapper rootMapper : rootMappersOrdered) {
                rootMapper.preParse(context);
            }

            if (!emptyDoc) {
                rootObjectMapper.parse(context);
            }

            for (int i = 0; i < countDownTokens; i++) {
                parser.nextToken();
            }

            for (RootMapper rootMapper : rootMappersOrdered) {
                rootMapper.postParse(context);
            }

            for (RootMapper rootMapper : rootMappersOrdered) {
                rootMapper.validate(context);
            }
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
        ParsedDocument doc = new ParsedDocument(context.uid(), context.id(), context.type(), source.routing(), source.timestamp(), source.ttl(), context.docs(), context.analyzer(),
                context.source(), context.mappersAdded()).parent(source.parent());
        // reset the context to free up memory
        context.reset(null, null, null, null);
        return doc;
    }

    public void addFieldMapper(FieldMapper fieldMapper) {
        synchronized (mutex) {
            fieldMappers = fieldMappers.concat(this, fieldMapper);
        }
        for (FieldMapperListener listener : fieldMapperListeners) {
            listener.fieldMapper(fieldMapper);
        }
    }

    public void addFieldMapperListener(FieldMapperListener fieldMapperListener, boolean includeExisting) {
        fieldMapperListeners.add(fieldMapperListener);
        if (includeExisting) {
            for (RootMapper rootMapper : rootMappersOrdered) {
                if (!rootMapper.includeInObject() && rootMapper instanceof FieldMapper) {
                    fieldMapperListener.fieldMapper((FieldMapper) rootMapper);
                }
            }
            rootObjectMapper.traverse(fieldMapperListener);
        }
    }

    public void addObjectMapper(ObjectMapper objectMapper) {
        synchronized (mutex) {
            objectMappers = MapBuilder.newMapBuilder(objectMappers).put(objectMapper.fullPath(), objectMapper).immutableMap();
            if (objectMapper.nested().isNested()) {
                hasNestedObjects = true;
            }
        }
        for (ObjectMapperListener objectMapperListener : objectMapperListeners) {
            objectMapperListener.objectMapper(objectMapper);
        }
    }

    public void addObjectMapperListener(ObjectMapperListener objectMapperListener, boolean includeExisting) {
        objectMapperListeners.add(objectMapperListener);
        if (includeExisting) {
            rootObjectMapper.traverse(objectMapperListener);
        }
    }

    public synchronized MergeResult merge(DocumentMapper mergeWith, MergeFlags mergeFlags) {
        MergeContext mergeContext = new MergeContext(this, mergeFlags);
        rootObjectMapper.merge(mergeWith.rootObjectMapper, mergeContext);

        for (Map.Entry<Class<? extends RootMapper>, RootMapper> entry : rootMappers.entrySet()) {
            // root mappers included in root object will get merge in the rootObjectMapper
            if (entry.getValue().includeInObject()) {
                continue;
            }
            RootMapper mergeWithRootMapper = mergeWith.rootMappers.get(entry.getKey());
            if (mergeWithRootMapper != null) {
                entry.getValue().merge(mergeWithRootMapper, mergeContext);
            }
        }

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
            this.mappingSource = new CompressedString(builder.bytes());
        } catch (Exception e) {
            throw new FailedToGenerateSourceMapperException(e.getMessage(), e);
        }
    }

    public void close() {
        cache.remove();
        rootObjectMapper.close();
        for (RootMapper rootMapper : rootMappersOrdered) {
            rootMapper.close();
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        rootObjectMapper.toXContent(builder, params, new ToXContent() {
            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
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
        }, rootMappersNotIncludedInObject);
        return builder;
    }
}
