/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

import java.util.List;

public class DocumentMapper {
    static final NodeFeature INDEX_SORTING_ON_NESTED = new NodeFeature("mapper.index_sorting_on_nested");

    private final String type;
    private final CompressedXContent mappingSource;
    private final MappingLookup mappingLookup;
    private final DocumentParser documentParser;
    private final MapperMetrics mapperMetrics;
    private final IndexVersion indexVersion;
    private final Logger logger;

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
        return new DocumentMapper(
            mapperService.documentParser(),
            mapping,
            mapping.toCompressedXContent(),
            IndexVersion.current(),
            mapperService.getMapperMetrics(),
            mapperService.index().getName()
        );
    }

    DocumentMapper(
        DocumentParser documentParser,
        Mapping mapping,
        CompressedXContent source,
        IndexVersion version,
        MapperMetrics mapperMetrics,
        String indexName
    ) {
        this.documentParser = documentParser;
        this.type = mapping.getRoot().fullPath();
        this.mappingLookup = MappingLookup.fromMapping(mapping);
        this.mappingSource = source;
        this.mapperMetrics = mapperMetrics;
        this.indexVersion = version;
        this.logger = Loggers.getLogger(getClass(), indexName);

        assert mapping.toCompressedXContent().equals(source) || isSyntheticSourceMalformed(source, version)
            : "provided source [" + source + "] differs from mapping [" + mapping.toCompressedXContent() + "]";
    }

    private void maybeLog(Exception ex) {
        if (logger.isDebugEnabled()) {
            logger.debug("Error while parsing document: " + ex.getMessage(), ex);
        } else if (IntervalThrottler.DOCUMENT_PARSING_FAILURE.accept()) {
            logger.error("Error while parsing document: " + ex.getMessage(), ex);
        }
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
        try {
            return documentParser.parseDocument(source, mappingLookup);
        } catch (Exception e) {
            maybeLog(e);
            throw e;
        }
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
        try {
            sourceMapper().newSourceLoader(mapping(), mapperMetrics.sourceFieldMetrics());
        } catch (IllegalArgumentException e) {
            mapperMetrics.sourceFieldMetrics().recordSyntheticSourceIncompatibleMapping();
            throw e;
        }

        if (settings.getIndexSortConfig().hasIndexSort() && mappers().nestedLookup() != NestedLookup.EMPTY) {
            if (indexVersion.before(IndexVersions.INDEX_SORTING_ON_NESTED)) {
                throw new IllegalArgumentException("cannot have nested fields when index sort is activated");
            }
            for (String field : settings.getValue(IndexSortConfig.INDEX_SORT_FIELD_SETTING)) {
                for (NestedObjectMapper nestedObjectMapper : mappers().nestedLookup().getNestedMappers().values()) {
                    if (field.startsWith(nestedObjectMapper.fullPath())) {
                        throw new IllegalArgumentException(
                            "cannot apply index sort to field [" + field + "] under nested object [" + nestedObjectMapper.fullPath() + "]"
                        );
                    }
                }
            }
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
                        "All fields that match routing_path must be configured with [time_series_dimension: true] "
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
