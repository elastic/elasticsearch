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
import org.elasticsearch.cluster.routing.RoutingField;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class DocumentMapper {
    private final String type;
    private final CompressedXContent mappingSource;
    private final MappingLookup mappingLookup;
    private final DocumentParser documentParser;
    private final MapperMetrics mapperMetrics;
    private final IndexVersion indexVersion;
    private final Logger logger;
    private final String indexName;

    /**
     * Create a new {@link DocumentMapper} that holds empty mappings.
     * @param mapperService the mapper service that holds the needed components
     * @return the newly created document mapper
     */
    public static DocumentMapper createEmpty(MapperService mapperService) {
        RootObjectMapper root = new RootObjectMapper.Builder(
            MapperService.SINGLE_MAPPING_NAME,
            mapperService.getIndexMode().isStrictColumnar() ? ObjectMapper.Defaults.SUBOBJECTS_COLUMNAR : ObjectMapper.Defaults.SUBOBJECTS
        ).build(
            MapperBuilderContext.root(
                false,
                false,
                MapperService.MergeReason.MAPPING_UPDATE,
                mapperService.getIndexMode().isStrictColumnar(),
                false
            )
        );
        MetadataFieldMapper[] metadata = mapperService.getMetadataBuilders()
            .values()
            .stream()
            .map(MetadataFieldMapper.Builder::build)
            .toArray(MetadataFieldMapper[]::new);
        Mapping mapping = new Mapping(root, metadata, null);
        return new DocumentMapper(
            mapperService.documentParser(),
            mapping,
            mapping.toCompressedXContent(),
            IndexVersion.current(),
            mapperService.getMapperMetrics(),
            mapperService.index().getName(),
            mapperService.getIndexMode()
        );
    }

    DocumentMapper(
        DocumentParser documentParser,
        Mapping mapping,
        CompressedXContent source,
        IndexVersion version,
        MapperMetrics mapperMetrics,
        String indexName,
        IndexMode indexMode
    ) {
        this.documentParser = documentParser;
        this.type = mapping.getRoot().fullPath();
        this.mappingLookup = MappingLookup.fromMapping(mapping, indexMode);
        this.mappingSource = source;
        this.mapperMetrics = mapperMetrics;
        this.indexVersion = version;
        this.logger = Loggers.getLogger(getClass(), indexName);
        this.indexName = indexName;
    }

    private void maybeLog(Exception ex) {
        if (logger.isDebugEnabled()) {
            logger.debug("Error while parsing document for index [" + indexName + "]: " + ex.getMessage(), ex);
        }
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
        if (settings.isSliceEnabled() && (routingFieldMapper().required() == false || routingFieldMapper().docValues() == false)) {
            throw new IllegalArgumentException(
                "mapping type ["
                    + type()
                    + "] must not configure [_routing] settings when ["
                    + IndexSettings.SLICE_ENABLED.getKey()
                    + "] is true for index ["
                    + settings.getIndex().getName()
                    + "]"
            );
        }

        validateRoutingField(settings);
        settings.getMode().validateMapping(mappingLookup, settings.getSettings());
        /*
         * Build an empty source loader to validate that the mapping is compatible
         * with the source loading strategy declared on the source field mapper.
         */
        try {
            mappingLookup.newSourceLoader(null, mapperMetrics.sourceFieldMetrics(), null);
        } catch (IllegalArgumentException e) {
            mapperMetrics.sourceFieldMetrics().recordSyntheticSourceIncompatibleMapping();
            throw e;
        }

        if (settings.getIndexSortConfig().hasIndexSort() && mappers().nestedLookup() != NestedLookup.EMPTY) {
            if (indexVersion.before(IndexVersions.INDEX_SORTING_ON_NESTED)) {
                throw new IllegalArgumentException("cannot have nested fields when index sort is activated");
            }
            for (String field : settings.getValue(IndexSortConfig.INDEX_SORT_FIELD_SETTING)) {
                NestedObjectMapper nestedMapper = mappers().nestedLookup().getNestedMappers().get(field);
                String nestedParent = nestedMapper != null ? nestedMapper.fullPath() : mappers().nestedLookup().getNestedParent(field);
                if (nestedParent != null) {
                    throw new IllegalArgumentException(
                        "cannot apply index sort to field [" + field + "] under nested object [" + nestedParent + "]"
                    );
                }
            }
        }
        if (settings.getIndexSortConfig().hasIndexSort()) {
            Set<String> sortFields = Set.copyOf(settings.getValue(IndexSortConfig.INDEX_SORT_FIELD_SETTING));
            Collection<RuntimeField> runtimeFields = mapping().getRoot().runtimeFields();
            for (RuntimeField rf : runtimeFields) {
                for (MappedFieldType ft : rf.asMappedFieldTypes().toList()) {
                    if (sortFields.contains(ft.name())) {
                        throw new MapperParsingException(
                            "runtime field ["
                                + ft.name()
                                + "] shadows an index sort field, "
                                + "which would prevent shards from being allocated"
                        );
                    }
                }
            }
        }
        List<String> routingPaths = settings.getIndexMetadata().getRoutingPaths();
        for (String path : routingPaths) {
            if (settings.getMode().isTsdb()) {
                for (String match : mappingLookup.getMatchingFieldNames(path)) {
                    mappingLookup.getFieldType(match).validateMatchedRoutingPath(path);
                }
            }
            for (String objectName : mappingLookup.objectMappers().keySet()) {
                // object type is not allowed in the routing paths
                if (path.equals(objectName)) {
                    throw new IllegalArgumentException(
                        "All fields that match routing_path must be flattened fields. [" + objectName + "] was [object]."
                    );
                }
            }
        }
        if (checkLimits) {
            this.mappingLookup.checkLimits(settings);
        }
    }

    /**
     * With {@link IndexSettings#ROUTING_FIELD}, routing must be required and the routing field mapped with a type whose
     * values can be read back from the source exactly: a keyword without normalizer, an integer type or an ip, and not a
     * sub-field (multi-fields have no value of their own in the source).
     */
    private void validateRoutingField(IndexSettings settings) {
        String path = IndexSettings.ROUTING_FIELD.get(settings.getSettings());
        if (path.isEmpty()) {
            return;
        }
        String index = settings.getIndex().getName();
        if (routingFieldMapper().required() == false) {
            throw new IllegalArgumentException(
                "[_routing] must be required in the mapping of index ["
                    + index
                    + "] because ["
                    + IndexSettings.ROUTING_FIELD.getKey()
                    + "] is set"
            );
        }
        MappedFieldType fieldType = mappingLookup.getFieldType(path);
        if (fieldType == null || mappingLookup.isMultiField(path) || RoutingField.Type.forFieldType(fieldType.typeName()) == null) {
            throw new IllegalArgumentException(
                "routing field ["
                    + path
                    + "] of index ["
                    + index
                    + "] must be mapped as one of "
                    + RoutingField.SUPPORTED_FIELD_TYPES.stream().sorted().toList()
                    + (fieldType == null ? ", but it is not mapped" : ", found [" + fieldType.typeName() + "]")
            );
        }
        if (fieldType instanceof KeywordFieldMapper.KeywordFieldType keyword && keyword.hasNormalizer()) {
            throw new IllegalArgumentException("routing field [" + path + "] of index [" + index + "] must not have a normalizer");
        }
    }
}
