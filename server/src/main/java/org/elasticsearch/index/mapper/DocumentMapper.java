/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

import java.util.List;

public class DocumentMapper {
    private final String type;
    private final CompressedXContent mappingSource;
    private final MappingLookup mappingLookup;
    private final DocumentParser documentParser;

    /**
     * Create a new {@link DocumentMapper} that holds empty mappings.
     * @param mapperService the mapper service that holds the needed components
     * @return the newly created document mapper
     */
    public static DocumentMapper createEmpty(MapperService mapperService) {
        RootObjectMapper root = new RootObjectMapper.Builder(MapperService.SINGLE_MAPPING_NAME, ObjectMapper.Defaults.SUBOBJECTS).build(
            MapperBuilderContext.root(false, false)
        );
        MetadataFieldMapper[] metadata = mapperService.getMetadataMappers().values().toArray(new MetadataFieldMapper[0]);
        Mapping mapping = new Mapping(root, metadata, null);
        return new DocumentMapper(mapperService.documentParser(), mapping, mapping.toCompressedXContent(), IndexVersion.current());
    }

    DocumentMapper(DocumentParser documentParser, Mapping mapping, CompressedXContent source, IndexVersion version) {
        this.documentParser = documentParser;
        this.type = mapping.getRoot().name();
        this.mappingLookup = MappingLookup.fromMapping(mapping);
        this.mappingSource = source;

        assert mapping.toCompressedXContent().equals(source) || isSyntheticSourceMalformed(source, version)
            : "provided source [" + source + "] differs from mapping [" + mapping.toCompressedXContent() + "]";
    }

    /**
     * Indexes built at v.8.7 were missing an explicit entry for synthetic_source.
     * This got restored in v.8.10 to avoid confusion. The change is only restricted to mapping printout, it has no
     * functional effect as the synthetic source already applies.
     */
    boolean isSyntheticSourceMalformed(CompressedXContent source, IndexVersion version) {
        return sourceMapper().isSynthetic()
            && source.string().contains("\"_source\":{\"mode\":\"synthetic\"}") == false
            && version.onOrBefore(IndexVersions.V_8_10_0);
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

    public RoutingFieldMapper routingFieldMapper() {
        return metadataMapper(RoutingFieldMapper.class);
    }

    public IndexFieldMapper IndexFieldMapper() {
        return metadataMapper(IndexFieldMapper.class);
    }

    public MappingLookup mappers() {
        return this.mappingLookup;
    }

    public ParsedDocument parse(SourceToParse source) throws DocumentParsingException {
        return documentParser.parseDocument(source, mappingLookup);
    }

    public void validate(IndexSettings settings, boolean checkLimits) {
        this.mapping().validate(this.mappingLookup);
        if (settings.getIndexMetadata().isRoutingPartitionedIndex()) {
            if (routingFieldMapper().required() == false) {
                throw new IllegalArgumentException(
                    "mapping type ["
                        + type()
                        + "] must have routing "
                        + "required for partitioned index ["
                        + settings.getIndex().getName()
                        + "]"
                );
            }
        }

        settings.getMode().validateMapping(mappingLookup);
        /*
         * Build an empty source loader to validate that the mapping is compatible
         * with the source loading strategy declared on the source field mapper.
         */
        sourceMapper().newSourceLoader(mapping());
        if (settings.getIndexSortConfig().hasIndexSort() && mappers().nestedLookup() != NestedLookup.EMPTY) {
            throw new IllegalArgumentException("cannot have nested fields when index sort is activated");
        }
        List<String> routingPaths = settings.getIndexMetadata().getRoutingPaths();
        for (String path : routingPaths) {
            for (String match : mappingLookup.getMatchingFieldNames(path)) {
                mappingLookup.getFieldType(match).validateMatchedRoutingPath(path);
            }
            for (String objectName : mappingLookup.objectMappers().keySet()) {
                // object type is not allowed in the routing paths
                if (path.equals(objectName)) {
                    throw new IllegalArgumentException(
                        "All fields that match routing_path must be keywords with [time_series_dimension: true] "
                            + "or flattened fields with a list of dimensions in [time_series_dimensions] "
                            + "and without the [script] parameter. ["
                            + objectName
                            + "] was [object]."
                    );
                }
            }
        }
        if (checkLimits) {
            this.mappingLookup.checkLimits(settings);
        }
    }
}
