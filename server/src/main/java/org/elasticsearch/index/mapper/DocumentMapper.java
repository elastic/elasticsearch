/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.analysis.IndexAnalyzers;

public class DocumentMapper {
    private final String type;
    private final CompressedXContent mappingSource;
    private final MappingLookup mappingLookup;

    /**
     * Create a new {@link DocumentMapper} that holds empty mappings.
     * @param mapperService the mapper service that holds the needed components
     * @return the newly created document mapper
     */
    public static DocumentMapper createEmpty(MapperService mapperService) {
        RootObjectMapper root = new RootObjectMapper.Builder(MapperService.SINGLE_MAPPING_NAME, Version.CURRENT).build(new ContentPath(1));
        MetadataFieldMapper[] metadata = mapperService.getMetadataMappers().values().toArray(new MetadataFieldMapper[0]);
        Mapping mapping = new Mapping(root, metadata, null);
        return new DocumentMapper(
            mapperService.getIndexSettings(), mapperService.getIndexAnalyzers(), mapperService.documentParser(), mapping);
    }

    DocumentMapper(IndexSettings indexSettings,
                   IndexAnalyzers indexAnalyzers,
                   DocumentParser documentParser,
                   Mapping mapping) {
        this.type = mapping.getRoot().name();
        this.mappingLookup = MappingLookup.fromMapping(mapping, documentParser, indexSettings, indexAnalyzers);
        this.mappingSource = mapping.toCompressedXContent();
    }

    public Mapping mapping() {
        return mappingLookup.getMapping();
    }

    public String type() {
        return this.type;
    }

    public CompressedXContent mappingSource() {
        return this.mappingSource;
    }

    public <T extends MetadataFieldMapper> T metadataMapper(Class<T> type) {
        return mapping().getMetadataMapperByClass(type);
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

    public MappingLookup mappers() {
        return this.mappingLookup;
    }

    public ParsedDocument parse(SourceToParse source) throws MapperParsingException {
        return mappingLookup.parseDocument(source);
    }

    public void validate(IndexSettings settings, boolean checkLimits) {
        this.mapping().validate(this.mappingLookup);
        if (settings.getIndexMetadata().isRoutingPartitionedIndex()) {
            if (routingFieldMapper().required() == false) {
                throw new IllegalArgumentException("mapping type [" + type() + "] must have routing "
                    + "required for partitioned index [" + settings.getIndex().getName() + "]");
            }
        }
        if (settings.getIndexSortConfig().hasIndexSort() && mappers().hasNested()) {
            throw new IllegalArgumentException("cannot have nested fields when index sort is activated");
        }
        if (checkLimits) {
            this.mappingLookup.checkLimits(settings);
        }
    }
}
