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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchGenerationException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;
import org.elasticsearch.index.mapper.MapperService.MergeReason;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;


public class DocumentMapper implements ToXContentFragment {

    public static final class Builder {
        private final Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers;
        private final RootObjectMapper rootObjectMapper;
        private final ContentPath contentPath;
        private final IndexSettings indexSettings;
        private final IndexAnalyzers indexAnalyzers;
        private final DocumentParser documentParser;

        private Map<String, Object> meta;

        public Builder(RootObjectMapper.Builder builder, MapperService mapperService) {
            this(builder, mapperService.getIndexSettings(), mapperService.getIndexAnalyzers(), mapperService.documentParser(),
                mapperService.getMetadataMappers());
        }

        Builder(RootObjectMapper.Builder builder,
                IndexSettings indexSettings,
                IndexAnalyzers indexAnalyzers,
                DocumentParser documentParser,
                Map<Class<? extends MetadataFieldMapper>, MetadataFieldMapper> metadataMappers) {
            this.indexSettings = indexSettings;
            this.indexAnalyzers = indexAnalyzers;
            this.documentParser = documentParser;
            this.contentPath = new ContentPath(1);
            this.rootObjectMapper = builder.build(contentPath);
            this.metadataMappers = metadataMappers;
        }

        public Builder meta(Map<String, Object> meta) {
            this.meta = meta;
            return this;
        }

        public Builder put(MetadataFieldMapper.Builder mapper) {
            MetadataFieldMapper metadataMapper = mapper.build(contentPath);
            metadataMappers.put(metadataMapper.getClass(), metadataMapper);
            return this;
        }

        public DocumentMapper build() {
            Objects.requireNonNull(rootObjectMapper, "Mapper builder must have the root object mapper set");
            Mapping mapping = new Mapping(
                    rootObjectMapper,
                    metadataMappers.values().toArray(new MetadataFieldMapper[0]),
                    meta);
            return new DocumentMapper(indexSettings, indexAnalyzers, documentParser, mapping);
        }
    }

    private final String type;
    private final Text typeText;
    private final CompressedXContent mappingSource;
    private final Mapping mapping;
    private final DocumentParser documentParser;
    private final MappingLookup fieldMappers;
    private final IndexSettings indexSettings;
    private final IndexAnalyzers indexAnalyzers;
    private final MetadataFieldMapper[] deleteTombstoneMetadataFieldMappers;
    private final MetadataFieldMapper[] noopTombstoneMetadataFieldMappers;

    private DocumentMapper(IndexSettings indexSettings,
                           IndexAnalyzers indexAnalyzers,
                           DocumentParser documentParser,
                           Mapping mapping) {
        this.type = mapping.root().name();
        this.typeText = new Text(this.type);
        this.mapping = mapping;
        this.documentParser = documentParser;
        this.indexSettings = indexSettings;
        this.indexAnalyzers = indexAnalyzers;
        this.fieldMappers = MappingLookup.fromMapping(mapping, this::parse);

        try {
            mappingSource = new CompressedXContent(this, XContentType.JSON, ToXContent.EMPTY_PARAMS);
        } catch (Exception e) {
            throw new ElasticsearchGenerationException("failed to serialize source for type [" + type + "]", e);
        }

        final Collection<String> deleteTombstoneMetadataFields = Arrays.asList(VersionFieldMapper.NAME, IdFieldMapper.NAME,
            SeqNoFieldMapper.NAME, SeqNoFieldMapper.PRIMARY_TERM_NAME, SeqNoFieldMapper.TOMBSTONE_NAME);
        this.deleteTombstoneMetadataFieldMappers = Stream.of(mapping.metadataMappers)
            .filter(field -> deleteTombstoneMetadataFields.contains(field.name())).toArray(MetadataFieldMapper[]::new);
        final Collection<String> noopTombstoneMetadataFields = Arrays.asList(
            VersionFieldMapper.NAME, SeqNoFieldMapper.NAME, SeqNoFieldMapper.PRIMARY_TERM_NAME, SeqNoFieldMapper.TOMBSTONE_NAME);
        this.noopTombstoneMetadataFieldMappers = Stream.of(mapping.metadataMappers)
            .filter(field -> noopTombstoneMetadataFields.contains(field.name())).toArray(MetadataFieldMapper[]::new);
    }

    IndexSettings indexSettings() {
        return indexSettings;
    }

    IndexAnalyzers indexAnalyzers() {
        return indexAnalyzers;
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
        return mappers().hasNested();
    }

    public MappingLookup mappers() {
        return this.fieldMappers;
    }

    public ParsedDocument parse(SourceToParse source) throws MapperParsingException {
        return documentParser.parseDocument(source, mapping.metadataMappers, this);
    }

    public ParsedDocument createDeleteTombstoneDoc(String index, String id) throws MapperParsingException {
        final SourceToParse emptySource = new SourceToParse(index, id, new BytesArray("{}"), XContentType.JSON);
        return documentParser.parseDocument(emptySource, deleteTombstoneMetadataFieldMappers, this).toTombstone();
    }

    public ParsedDocument createNoopTombstoneDoc(String index, String reason) throws MapperParsingException {
        final String id = ""; // _id won't be used.
        final SourceToParse sourceToParse = new SourceToParse(index, id, new BytesArray("{}"), XContentType.JSON);
        final ParsedDocument parsedDoc = documentParser.parseDocument(sourceToParse, noopTombstoneMetadataFieldMappers, this).toTombstone();
        // Store the reason of a noop as a raw string in the _source field
        final BytesRef byteRef = new BytesRef(reason);
        parsedDoc.rootDoc().add(new StoredField(SourceFieldMapper.NAME, byteRef.bytes, byteRef.offset, byteRef.length));
        return parsedDoc;
    }

    /**
     * Given an object path, checks to see if any of its parents are non-nested objects
     */
    public boolean hasNonNestedParent(String path) {
        ObjectMapper mapper = mappers().objectMappers().get(path);
        if (mapper == null) {
            return false;
        }
        while (mapper != null) {
            if (mapper.nested().isNested() == false) {
                return true;
            }
            if (path.contains(".") == false) {
                return false;
            }
            path = path.substring(0, path.lastIndexOf("."));
            mapper = mappers().objectMappers().get(path);
        }
        return false;
    }

    /**
     * Returns all nested object mappers
     */
    public List<ObjectMapper> getNestedMappers() {
        List<ObjectMapper> childMappers = new ArrayList<>();
        for (ObjectMapper mapper : mappers().objectMappers().values()) {
            if (mapper.nested().isNested() == false) {
                continue;
            }
            childMappers.add(mapper);
        }
        return childMappers;
    }

    /**
     * Returns all nested object mappers which contain further nested object mappers
     *
     * Used by BitSetProducerWarmer
     */
    public List<ObjectMapper> getNestedParentMappers() {
        List<ObjectMapper> parents = new ArrayList<>();
        for (ObjectMapper mapper : mappers().objectMappers().values()) {
            String nestedParentPath = getNestedParent(mapper.fullPath());
            if (nestedParentPath == null) {
                continue;
            }
            ObjectMapper parent = mappers().objectMappers().get(nestedParentPath);
            if (parent.nested().isNested()) {
                parents.add(parent);
            }
        }
        return parents;
    }

    /**
     * Given a nested object path, returns the path to its nested parent
     *
     * In particular, if a nested field `foo` contains an object field
     * `bar.baz`, then calling this method with `foo.bar.baz` will return
     * the path `foo`, skipping over the object-but-not-nested `foo.bar`
     */
    public String getNestedParent(String path) {
        ObjectMapper mapper = mappers().objectMappers().get(path);
        if (mapper == null) {
            return null;
        }
        if (path.contains(".") == false) {
            return null;
        }
        do {
            path = path.substring(0, path.lastIndexOf("."));
            mapper = mappers().objectMappers().get(path);
            if (mapper == null) {
                return null;
            }
            if (mapper.nested().isNested()) {
                return path;
            }
            if (path.contains(".") == false) {
                return null;
            }
        } while(true);
    }

    public DocumentMapper merge(Mapping mapping, MergeReason reason) {
        Mapping merged = this.mapping.merge(mapping, reason);
        return new DocumentMapper(this.indexSettings, this.indexAnalyzers, this.documentParser, merged);
    }

    public void validate(IndexSettings settings, boolean checkLimits) {
        this.mapping.validate(this.fieldMappers);
        if (settings.getIndexMetadata().isRoutingPartitionedIndex()) {
            if (routingFieldMapper().required() == false) {
                throw new IllegalArgumentException("mapping type [" + type() + "] must have routing "
                    + "required for partitioned index [" + settings.getIndex().getName() + "]");
            }
        }
        if (settings.getIndexSortConfig().hasIndexSort() && hasNestedObjects()) {
            throw new IllegalArgumentException("cannot have nested fields when index sort is activated");
        }
        if (checkLimits) {
            this.fieldMappers.checkLimits(settings);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return mapping.toXContent(builder, params);
    }
}
