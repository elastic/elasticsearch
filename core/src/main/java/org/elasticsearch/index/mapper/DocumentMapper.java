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

import com.google.common.collect.ImmutableMap;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.analysis.AnalysisService;
import org.elasticsearch.index.mapper.Mapping.SourceTransform;
import org.elasticsearch.index.mapper.MetadataFieldMapper.TypeParser;
import org.elasticsearch.index.mapper.internal.AllFieldMapper;
import org.elasticsearch.index.mapper.internal.IdFieldMapper;
import org.elasticsearch.index.mapper.internal.IndexFieldMapper;
import org.elasticsearch.index.mapper.internal.ParentFieldMapper;
import org.elasticsearch.index.mapper.internal.RoutingFieldMapper;
import org.elasticsearch.index.mapper.internal.SourceFieldMapper;
import org.elasticsearch.index.mapper.internal.TTLFieldMapper;
import org.elasticsearch.index.mapper.internal.TimestampFieldMapper;
import org.elasticsearch.index.mapper.internal.TypeFieldMapper;
import org.elasticsearch.index.mapper.internal.UidFieldMapper;
import org.elasticsearch.index.mapper.object.ObjectMapper;
import org.elasticsearch.index.mapper.object.RootObjectMapper;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 *
 */
public class DocumentMapper implements ToXContent {

    public static class Builder {

        private Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers = new LinkedHashMap<>();

        private List<SourceTransform> sourceTransforms = new ArrayList<>(1);

        private final RootObjectMapper rootObjectMapper;

        private ImmutableMap<String, Object> meta = ImmutableMap.of();

        private final Mapper.BuilderContext builderContext;

        public Builder(RootObjectMapper.Builder builder, MapperService mapperService) {
            final Settings indexSettings = mapperService.indexSettings();
            this.builderContext = new Mapper.BuilderContext(indexSettings, new ContentPath(1));
            this.rootObjectMapper = builder.build(builderContext);

            final String type = rootObjectMapper.name();
            DocumentMapper existingMapper = mapperService.documentMapper(type);
            for (Map.Entry<String, MetadataFieldMapper.TypeParser> entry : mapperService.mapperRegistry.getMetadataMapperParsers().entrySet()) {
                final String name = entry.getKey();
                final MetadataFieldMapper existingMetadataMapper = existingMapper == null
                        ? null
                        : (MetadataFieldMapper) existingMapper.mappers().getMapper(name);
                final MetadataFieldMapper metadataMapper;
                if (existingMetadataMapper == null) {
                    final TypeParser parser = entry.getValue();
                    metadataMapper = parser.getDefault(indexSettings, mapperService.fullName(name), builder.name());
                } else {
                    metadataMapper = existingMetadataMapper;
                }
                metadataMappers.put(metadataMapper.getClass(), metadataMapper);
            }
        }

        public Builder meta(ImmutableMap<String, Object> meta) {
            this.meta = meta;
            return this;
        }

        public Builder put(MetadataFieldMapper.Builder<?, ?> mapper) {
            MetadataFieldMapper metadataMapper = mapper.build(builderContext);
            metadataMappers.put(metadataMapper.getClass(), metadataMapper);
            return this;
        }

        public Builder transform(ScriptService scriptService, Script script) {
            sourceTransforms.add(new ScriptTransform(scriptService, script));
            return this;
        }

        /**
         * @deprecated Use {@link #transform(ScriptService, Script)} instead.
         */
        @Deprecated
        public Builder transform(ScriptService scriptService, String script, ScriptType scriptType, String language,
                Map<String, Object> parameters) {
            sourceTransforms.add(new ScriptTransform(scriptService, new Script(script, scriptType, language, parameters)));
            return this;
        }

        public DocumentMapper build(MapperService mapperService) {
            Objects.requireNonNull(rootObjectMapper, "Mapper builder must have the root object mapper set");
            Mapping mapping = new Mapping(
                    Version.indexCreated(mapperService.indexSettings()),
                    rootObjectMapper,
                    metadataMappers.values().toArray(new MetadataFieldMapper[metadataMappers.values().size()]),
                    sourceTransforms.toArray(new SourceTransform[sourceTransforms.size()]),
                    meta);
            return new DocumentMapper(mapperService, mapping);
        }
    }

    private final MapperService mapperService;

    private final String type;
    private final Text typeText;

    private final CompressedXContent mappingSource;

    private final Mapping mapping;

    private final DocumentParser documentParser;

    private final DocumentFieldMappers fieldMappers;

    private final Map<String, ObjectMapper> objectMappers;

    private final boolean hasNestedObjects;

    public DocumentMapper(MapperService mapperService, Mapping mapping) {
        this.mapperService = mapperService;
        this.type = mapping.root().name();
        this.typeText = new Text(this.type);
        this.mapping = mapping;
        this.documentParser = new DocumentParser(mapperService.indexSettings(), mapperService.documentMapperParser(), this);

        if (metadataMapper(ParentFieldMapper.class).active()) {
            // mark the routing field mapper as required
            metadataMapper(RoutingFieldMapper.class).markAsRequired();
        }

        // collect all the mappers for this type
        List<ObjectMapper> newObjectMappers = new ArrayList<>();
        List<FieldMapper> newFieldMappers = new ArrayList<>();
        for (MetadataFieldMapper metadataMapper : this.mapping.metadataMappers) {
            if (metadataMapper instanceof FieldMapper) {
                newFieldMappers.add(metadataMapper);
            }
        }
        MapperUtils.collect(this.mapping.root, newObjectMappers, newFieldMappers);

        final AnalysisService analysisService = mapperService.analysisService();
        this.fieldMappers = new DocumentFieldMappers(newFieldMappers,
                analysisService.defaultIndexAnalyzer(),
                analysisService.defaultSearchAnalyzer(),
                analysisService.defaultSearchQuoteAnalyzer());

        Map<String, ObjectMapper> builder = new HashMap<>();
        for (ObjectMapper objectMapper : newObjectMappers) {
            ObjectMapper previous = builder.put(objectMapper.fullPath(), objectMapper);
            if (previous != null) {
                throw new IllegalStateException("duplicate key " + objectMapper.fullPath() + " encountered");
            }
        }

        boolean hasNestedObjects = false;
        this.objectMappers = Collections.unmodifiableMap(builder);
        for (ObjectMapper objectMapper : newObjectMappers) {
            if (objectMapper.nested().isNested()) {
                hasNestedObjects = true;
            }
        }
        this.hasNestedObjects = hasNestedObjects;

        try {
            mappingSource = new CompressedXContent(this, XContentType.JSON, ToXContent.EMPTY_PARAMS);
        } catch (Exception e) {
            throw new ElasticsearchGenerationException("failed to serialize source for type [" + type + "]", e);
        }
    }

    public Mapping mapping() {
        return mapping;
    }

    public String type() {
        return this.type;
    }

    public Text typeText() {
        return this.typeText;
    }

    public ImmutableMap<String, Object> meta() {
        return mapping.meta;
    }

    public CompressedXContent mappingSource() {
        return this.mappingSource;
    }

    public RootObjectMapper root() {
        return mapping.root;
    }

    public UidFieldMapper uidMapper() {
        return metadataMapper(UidFieldMapper.class);
    }

    @SuppressWarnings({"unchecked"})
    public <T extends MetadataFieldMapper> T metadataMapper(Class<T> type) {
        return mapping.metadataMapper(type);
    }

    public IndexFieldMapper indexMapper() {
        return metadataMapper(IndexFieldMapper.class);
    }

    public TypeFieldMapper typeMapper() {
        return metadataMapper(TypeFieldMapper.class);
    }

    public SourceFieldMapper sourceMapper() {
        return metadataMapper(SourceFieldMapper.class);
    }

    public AllFieldMapper allFieldMapper() {
        return metadataMapper(AllFieldMapper.class);
    }

    public IdFieldMapper idFieldMapper() {
        return metadataMapper(IdFieldMapper.class);
    }

    public RoutingFieldMapper routingFieldMapper() {
        return metadataMapper(RoutingFieldMapper.class);
    }

    public ParentFieldMapper parentFieldMapper() {
        return metadataMapper(ParentFieldMapper.class);
    }

    public TimestampFieldMapper timestampFieldMapper() {
        return metadataMapper(TimestampFieldMapper.class);
    }

    public TTLFieldMapper TTLFieldMapper() {
        return metadataMapper(TTLFieldMapper.class);
    }

    public IndexFieldMapper IndexFieldMapper() {
        return metadataMapper(IndexFieldMapper.class);
    }

    public Query typeFilter() {
        return typeMapper().fieldType().termQuery(type, null);
    }

    public boolean hasNestedObjects() {
        return hasNestedObjects;
    }

    public DocumentFieldMappers mappers() {
        return this.fieldMappers;
    }

    public Map<String, ObjectMapper> objectMappers() {
        return this.objectMappers;
    }

    public ParsedDocument parse(String index, String type, String id, BytesReference source) throws MapperParsingException {
        return parse(SourceToParse.source(source).index(index).type(type).id(id));
    }

    public ParsedDocument parse(SourceToParse source) throws MapperParsingException {
        return documentParser.parseDocument(source);
    }

    /**
     * Returns the best nested {@link ObjectMapper} instances that is in the scope of the specified nested docId.
     */
    public ObjectMapper findNestedObjectMapper(int nestedDocId, SearchContext sc, LeafReaderContext context) throws IOException {
        ObjectMapper nestedObjectMapper = null;
        for (ObjectMapper objectMapper : objectMappers().values()) {
            if (!objectMapper.nested().isNested()) {
                continue;
            }

            Query filter = objectMapper.nestedTypeFilter();
            if (filter == null) {
                continue;
            }
            // We can pass down 'null' as acceptedDocs, because nestedDocId is a doc to be fetched and
            // therefor is guaranteed to be a live doc.
            final Weight nestedWeight = filter.createWeight(sc.searcher(), false);
            Scorer scorer = nestedWeight.scorer(context);
            if (scorer == null) {
                continue;
            }

            if (scorer.iterator().advance(nestedDocId) == nestedDocId) {
                if (nestedObjectMapper == null) {
                    nestedObjectMapper = objectMapper;
                } else {
                    if (nestedObjectMapper.fullPath().length() < objectMapper.fullPath().length()) {
                        nestedObjectMapper = objectMapper;
                    }
                }
            }
        }
        return nestedObjectMapper;
    }

    /**
     * Returns the parent {@link ObjectMapper} instance of the specified object mapper or <code>null</code> if there
     * isn't any.
     */
    // TODO: We should add: ObjectMapper#getParentObjectMapper()
    public ObjectMapper findParentObjectMapper(ObjectMapper objectMapper) {
        int indexOfLastDot = objectMapper.fullPath().lastIndexOf('.');
        if (indexOfLastDot != -1) {
            String parentNestObjectPath = objectMapper.fullPath().substring(0, indexOfLastDot);
            return objectMappers().get(parentNestObjectPath);
        } else {
            return null;
        }
    }

    /**
     * Transform the source when it is expressed as a map.  This is public so it can be transformed the source is loaded.
     * @param sourceAsMap source to transform.  This may be mutated by the script.
     * @return transformed version of transformMe.  This may actually be the same object as sourceAsMap
     */
    public Map<String, Object> transformSourceAsMap(Map<String, Object> sourceAsMap) {
        return DocumentParser.transformSourceAsMap(mapping, sourceAsMap);
    }

    public boolean isParent(String type) {
        return mapperService.getParentTypes().contains(type);
    }

    public DocumentMapper merge(Mapping mapping, boolean updateAllTypes) {
        Mapping merged = this.mapping.merge(mapping, updateAllTypes);
        return new DocumentMapper(mapperService, merged);
    }

    /**
     * Recursively update sub field types.
     */
    public DocumentMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        Mapping updated = this.mapping.updateFieldType(fullNameToFieldType);
        return new DocumentMapper(mapperService, updated);
    }

    public void close() {
        documentParser.close();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return mapping.toXContent(builder, params);
    }

    /**
     * Script based source transformation.
     */
    private static class ScriptTransform implements SourceTransform {
        private final ScriptService scriptService;
        /**
         * The script to transform the source document before indexing.
         */
        private final Script script;

        public ScriptTransform(ScriptService scriptService, Script script) {
            this.scriptService = scriptService;
            this.script = script;
        }

        @Override
        @SuppressWarnings("unchecked")
        public Map<String, Object> transformSourceAsMap(Map<String, Object> sourceAsMap) {
            try {
                // We use the ctx variable and the _source name to be consistent with the update api.
                ExecutableScript executable = scriptService.executable(script, ScriptContext.Standard.MAPPING, null, Collections.<String, String>emptyMap());
                Map<String, Object> ctx = new HashMap<>(1);
                ctx.put("_source", sourceAsMap);
                executable.setNextVar("ctx", ctx);
                executable.run();
                ctx = (Map<String, Object>) executable.unwrap(ctx);
                return (Map<String, Object>) ctx.get("_source");
            } catch (Exception e) {
                throw new IllegalArgumentException("failed to execute script", e);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return script.toXContent(builder, params);
        }
    }
}
