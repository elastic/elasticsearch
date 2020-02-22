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

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.mapper.MetadataFieldMapper.TypeParser;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;


public class DocumentMapper implements ToXContentFragment {

    public static class Builder {

        private Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers = new LinkedHashMap<>();

        private final RootObjectMapper rootObjectMapper;

        private Map<String, Object> meta;

        private final Mapper.BuilderContext builderContext;

        public Builder(RootObjectMapper.Builder builder, MapperService mapperService) {
            final Settings indexSettings = mapperService.getIndexSettings().getSettings();
            this.builderContext = new Mapper.BuilderContext(indexSettings, new ContentPath(1));
            this.rootObjectMapper = builder.build(builderContext);

            final String type = rootObjectMapper.name();
            final DocumentMapper existingMapper = mapperService.documentMapper();
            final Version indexCreatedVersion = mapperService.getIndexSettings().getIndexVersionCreated();
            final Map<String, TypeParser> metadataMapperParsers =
                mapperService.mapperRegistry.getMetadataMapperParsers(indexCreatedVersion);
            for (Map.Entry<String, MetadataFieldMapper.TypeParser> entry : metadataMapperParsers.entrySet()) {
                final String name = entry.getKey();
                final MetadataFieldMapper existingMetadataMapper = existingMapper == null
                        ? null
                        : (MetadataFieldMapper) existingMapper.mappers().getMapper(name);
                final MetadataFieldMapper metadataMapper;
                if (existingMetadataMapper == null) {
                    final TypeParser parser = entry.getValue();
                    metadataMapper = parser.getDefault(mapperService.documentMapperParser().parserContext());
                } else {
                    metadataMapper = existingMetadataMapper;
                }
                metadataMappers.put(metadataMapper.getClass(), metadataMapper);
            }
        }

        public Builder meta(Map<String, Object> meta) {
            this.meta = meta;
            return this;
        }

        public Builder put(MetadataFieldMapper.Builder<?, ?> mapper) {
            MetadataFieldMapper metadataMapper = mapper.build(builderContext);
            metadataMappers.put(metadataMapper.getClass(), metadataMapper);
            return this;
        }

        public DocumentMapper build(MapperService mapperService) {
            Objects.requireNonNull(rootObjectMapper, "Mapper builder must have the root object mapper set");
            Mapping mapping = new Mapping(
                    mapperService.getIndexSettings().getIndexVersionCreated(),
                    rootObjectMapper,
                    metadataMappers.values().toArray(new MetadataFieldMapper[metadataMappers.values().size()]),
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
    private final MetadataFieldMapper[] deleteTombstoneMetadataFieldMappers;
    private final MetadataFieldMapper[] noopTombstoneMetadataFieldMappers;

    public DocumentMapper(MapperService mapperService, Mapping mapping) {
        this.mapperService = mapperService;
        this.type = mapping.root().name();
        this.typeText = new Text(this.type);
        final IndexSettings indexSettings = mapperService.getIndexSettings();
        this.mapping = mapping;
        this.documentParser = new DocumentParser(indexSettings, mapperService.documentMapperParser(), this);

        // collect all the mappers for this type
        List<ObjectMapper> newObjectMappers = new ArrayList<>();
        List<FieldMapper> newFieldMappers = new ArrayList<>();
        List<FieldAliasMapper> newFieldAliasMappers = new ArrayList<>();
        for (MetadataFieldMapper metadataMapper : this.mapping.metadataMappers) {
            if (metadataMapper instanceof FieldMapper) {
                newFieldMappers.add(metadataMapper);
            }
        }
        MapperUtils.collect(this.mapping.root,
            newObjectMappers, newFieldMappers, newFieldAliasMappers);

        final IndexAnalyzers indexAnalyzers = mapperService.getIndexAnalyzers();
        this.fieldMappers = new DocumentFieldMappers(newFieldMappers,
                newFieldAliasMappers,
                indexAnalyzers.getDefaultIndexAnalyzer(),
                indexAnalyzers.getDefaultSearchAnalyzer(),
                indexAnalyzers.getDefaultSearchQuoteAnalyzer());

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

        final Collection<String> deleteTombstoneMetadataFields = Arrays.asList(VersionFieldMapper.NAME, IdFieldMapper.NAME,
            TypeFieldMapper.NAME, SeqNoFieldMapper.NAME, SeqNoFieldMapper.PRIMARY_TERM_NAME, SeqNoFieldMapper.TOMBSTONE_NAME);
        this.deleteTombstoneMetadataFieldMappers = Stream.of(mapping.metadataMappers)
            .filter(field -> deleteTombstoneMetadataFields.contains(field.name())).toArray(MetadataFieldMapper[]::new);
        final Collection<String> noopTombstoneMetadataFields = Arrays.asList(
            VersionFieldMapper.NAME, SeqNoFieldMapper.NAME, SeqNoFieldMapper.PRIMARY_TERM_NAME, SeqNoFieldMapper.TOMBSTONE_NAME);
        this.noopTombstoneMetadataFieldMappers = Stream.of(mapping.metadataMappers)
            .filter(field -> noopTombstoneMetadataFields.contains(field.name())).toArray(MetadataFieldMapper[]::new);
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

    public Map<String, Object> meta() {
        return mapping.meta;
    }

    public CompressedXContent mappingSource() {
        return this.mappingSource;
    }

    public RootObjectMapper root() {
        return mapping.root;
    }

    public <T extends MetadataFieldMapper> T metadataMapper(Class<T> type) {
        return mapping.metadataMapper(type);
    }

    public SourceFieldMapper sourceMapper() {
        return metadataMapper(SourceFieldMapper.class);
    }

    public IdFieldMapper idFieldMapper() {
        return metadataMapper(IdFieldMapper.class);
    }

    public RoutingFieldMapper routingFieldMapper() {
        return metadataMapper(RoutingFieldMapper.class);
    }

    public IndexFieldMapper IndexFieldMapper() {
        return metadataMapper(IndexFieldMapper.class);
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

    public ParsedDocument parse(SourceToParse source) throws MapperParsingException {
        return documentParser.parseDocument(source, mapping.metadataMappers);
    }

    public ParsedDocument createDeleteTombstoneDoc(String index, String id) throws MapperParsingException {
        final SourceToParse emptySource = new SourceToParse(index, id, new BytesArray("{}"), XContentType.JSON);
        return documentParser.parseDocument(emptySource, deleteTombstoneMetadataFieldMappers).toTombstone();
    }

    public ParsedDocument createNoopTombstoneDoc(String index, String reason) throws MapperParsingException {
        final String id = ""; // _id won't be used.
        final SourceToParse sourceToParse = new SourceToParse(index, id, new BytesArray("{}"), XContentType.JSON);
        final ParsedDocument parsedDoc = documentParser.parseDocument(sourceToParse, noopTombstoneMetadataFieldMappers).toTombstone();
        // Store the reason of a noop as a raw string in the _source field
        final BytesRef byteRef = new BytesRef(reason);
        parsedDoc.rootDoc().add(new StoredField(SourceFieldMapper.NAME, byteRef.bytes, byteRef.offset, byteRef.length));
        return parsedDoc;
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
            final Weight nestedWeight = filter.createWeight(sc.searcher(), ScoreMode.COMPLETE_NO_SCORES, 1f);
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

    public DocumentMapper merge(Mapping mapping) {
        Mapping merged = this.mapping.merge(mapping);
        return new DocumentMapper(mapperService, merged);
    }

    /**
     * Recursively update sub field types.
     */
    public DocumentMapper updateFieldType(Map<String, MappedFieldType> fullNameToFieldType) {
        Mapping updated = this.mapping.updateFieldType(fullNameToFieldType);
        if (updated == this.mapping) {
            // no change
            return this;
        }
        assert updated == updated.updateFieldType(fullNameToFieldType) : "updateFieldType operation is not idempotent";
        return new DocumentMapper(mapperService, updated);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return mapping.toXContent(builder, params);
    }
}
