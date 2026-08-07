/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.mapper.ColumnGroupResolver.ColumnGroupResolution;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceSchema;

/**
 * Batch-time mapper resolution and columnar batch mapping for the bulk batch-indexing fast path.
 *
 * <p>Workflow:
 * <ol>
 *     <li>{@link #resolveMappers(SourceSchema, MappingLookup, IndexSettings)} runs once per batch. It walks the
 *     schema leaves and binds each column to a {@link FieldMapper} (or records {@code null} for
 *     columns that are silently ignored under a {@code dynamic=false} parent). Leaves that belong to a
 *     {@link FieldMapper#resolvesColumnGroup() group mapper} (e.g. {@code flattened}) are accumulated
 *     separately in {@link BatchMapperResolution#columnGroups()}. Any configuration
 *     outside the v1 support matrix — runtime fields, index-time scripts, dynamic mapping,
 *     unsupported mapper types, etc. — causes the method to return {@code null}, at which point
 *     {@link ShardBatchIndexer} falls back to the sequential path.</li>
 *     <li>{@link #mapColumnBatch(BulkItemRequest[], SourceBatch, IndexShard, int, int, BatchMapperResolution, Engine.Operation.Origin)}
 *     runs per chunk. It invokes each mapper once for the whole chunk — attaching one Lucene column per batch-wide value
 *     (id, source, engine-assigned seq-no/version, ...) via {@link BatchMappingContext}, and assembles {@link Engine.Index} operations
 *     plus the resulting {@link EngineBatch}. After the per-leaf loop, each group mapper is dispatched via
 *     {@link FieldMapper#mapColumnGroupBatch}.</li>
 * </ol>
 */
public final class ShardBatchMapper {

    private static final Logger logger = LogManager.getLogger(ShardBatchMapper.class);

    private ShardBatchMapper() {}

    /**
     * Result of {@link #resolveMappers(SourceSchema, MappingLookup, IndexSettings)}.
     *
     * <p>{@code columnMappers} holds one entry per schema leaf; a {@code null} entry means the column is
     * silently ignored (nearest parent has {@code dynamic=false}) or the leaf is owned by a group mapper
     * and will be dispatched via {@code columnGroups} instead.
     *
     * <p>{@code columnGroups} holds one entry per group mapper (e.g. {@code flattened}), ordered by the
     * first schema leaf that mapped to that group.
     */
    public record BatchMapperResolution(FieldMapper[] columnMappers, ColumnGroupResolution[] columnGroups) {}

    /**
     * Resolve each schema leaf to a {@link FieldMapper}. Returns {@code null} if any scenario
     * falls outside the v1 batch-indexing support matrix and the caller should fall back to the
     * sequential path.
     */
    public static BatchMapperResolution resolveMappers(SourceSchema schema, MappingLookup lookup, IndexSettings indexSettings) {
        // Runtime fields or index-time scripts anywhere in the mapping would require the normal
        // parsing flow; the batch path does not support them.
        if (lookup.getMapping().getRoot().runtimeFields().isEmpty() == false) {
            logger.debug("batch indexing disabled: mapping defines runtime fields");
            return null;
        }
        if (lookup.indexTimeScriptMappers().isEmpty() == false) {
            logger.debug("batch indexing disabled: mapping defines index-time scripts");
            return null;
        }
        if (lookup.getMapping().getMetadataMapperByName(IdFieldMapper.NAME) instanceof SliceIdFieldMapper) {
            logger.debug("batch indexing disabled: slice-enabled index");
            return null;
        }

        for (MetadataFieldMapper mapper : lookup.getMapping().getSortedMetadataMappers()) {
            if (mapper.supportsColumnarMetadataParse(indexSettings) == false) {
                logger.debug(
                    "columnar batch mapping disabled: metadata mapper of type [{}] does not support columnar parsing",
                    mapper.typeName()
                );
                return null;
            }
        }

        final int leafCount = schema.leafCount();
        final FieldMapper[] columnMappers = new FieldMapper[leafCount];
        ColumnGroupResolver.Builder groupBuilder = null;

        for (int leaf = 0; leaf < leafCount; leaf++) {
            final String fullPath = schema.getFullPath(leaf);
            final Mapper resolved = lookup.getMapper(fullPath);

            if (resolved == null) {
                // Before the runtime-field-shadow check: a leaf under a group mapper (e.g. flattened) has
                // no mapper of its own, but MappingLookup.getFieldType may still return non-null because
                // the owning mapper is a DynamicFieldType. The group check must precede the shadow check or
                // every flattened batch is incorrectly classified as containing a runtime-field shadow.
                final ColumnGroupResolver.ColumnGroupMatch groupMatch = ColumnGroupResolver.findColumnGroup(fullPath, lookup);
                if (groupMatch != null) {
                    if (groupMatch.mapper().supportsColumnarParse(indexSettings) == false) {
                        logger.debug(
                            "columnar batch mapping disabled: group mapper at [{}] of type [{}] does not support columnar parsing",
                            groupMatch.ownerPath(),
                            groupMatch.mapper().typeName()
                        );
                        return null;
                    }
                    if (groupBuilder == null) {
                        groupBuilder = new ColumnGroupResolver.Builder();
                    }
                    groupBuilder.add(groupMatch, leaf);
                    // leaf is owned by the group mapper; no individual column mapper
                    columnMappers[leaf] = null;
                    continue;
                }

                // A field type without a mapper (and no group owner) indicates a runtime field shadow.
                if (lookup.getFieldType(fullPath) != null) {
                    logger.debug("batch indexing disabled: runtime-field shadow at [{}]", fullPath);
                    return null;
                }
                final ObjectMapper.Dynamic parentDynamic = findNearestParentDynamic(fullPath, lookup);
                if (parentDynamic == ObjectMapper.Dynamic.FALSE) {
                    // TODO: Look into ignored source
                    // leaf silently ignored
                    columnMappers[leaf] = null;
                    continue;
                }
                logger.debug("batch indexing disabled: unmapped leaf [{}] under dynamic={} parent", fullPath, parentDynamic);
                return null;
            }

            if ((resolved instanceof FieldMapper) == false) {
                logger.debug("batch indexing disabled: non-field mapper at [{}]", fullPath);
                return null;
            }
            final FieldMapper fieldMapper = (FieldMapper) resolved;
            if (fieldMapper.supportsColumnarParse(indexSettings) == false) {
                logger.debug(
                    "columnar batch mapping disabled: mapper at [{}] of type [{}] does not support columnar parsing",
                    fullPath,
                    fieldMapper.typeName()
                );
                return null;
            }
            columnMappers[leaf] = fieldMapper;
        }

        final ColumnGroupResolution[] columnGroups = groupBuilder != null ? groupBuilder.build() : ColumnGroupResolver.EMPTY;
        return new BatchMapperResolution(columnMappers, columnGroups);
    }

    /**
     * Walks up the parent-object chain for {@code leafPath}, returning the effective
     * {@link ObjectMapper.Dynamic} setting of the nearest ancestor that declares one, or the
     * root mapping's setting (defaulting to {@link ObjectMapper.Dynamic#TRUE}) if none do.
     */
    private static ObjectMapper.Dynamic findNearestParentDynamic(String leafPath, MappingLookup lookup) {
        String current = leafPath;
        while (true) {
            final int dot = current.lastIndexOf('.');
            if (dot <= 0) {
                break;
            }
            current = current.substring(0, dot);
            final ObjectMapper parent = lookup.objectMappers().get(current);
            if (parent != null && parent.dynamic() != null) {
                return parent.dynamic();
            }
        }
        // In COLUMNAR mode, objects are flattened (subobjects:DISABLED) and do not appear in
        // objectMappers(). Their dynamic settings are instead stored in prefixProperties on
        // RootObjectMapper. resolveDynamic() consults those when prefixProperties is non-empty,
        // and returns the fallback unchanged when it is empty (non-COLUMNAR path).
        final ObjectMapper.Dynamic rootDynamic = lookup.getMapping().getRoot().dynamic();
        final ObjectMapper.Dynamic rootFallback = rootDynamic == null ? ObjectMapper.Dynamic.TRUE : rootDynamic;
        return lookup.getMapping().getRoot().resolveDynamic(leafPath, rootFallback);
    }

    /**
     * Executes the columnar batch-mapping fast path for one chunk. Returns {@code null} (the
     * fallback signal — same contract as {@link #resolveMappers}) if mapping hits an unexpected
     * exception.
     */
    public static EngineBatch mapColumnBatch(
        BulkItemRequest[] items,
        SourceBatch batch,
        IndexShard shard,
        int chunkStart,
        int chunkEnd,
        BatchMapperResolution resolution,
        Engine.Operation.Origin origin
    ) {
        final MappingLookup mappingLookup = shard.mapperService().mappingLookup();
        final MetadataFieldMapper[] metadataMappers = mappingLookup.getMapping().getSortedMetadataMappers();

        final IndexOperationBatch indexBatch = IndexOperationBatch.initFromBulk(
            items,
            chunkStart,
            chunkEnd,
            batch.slice(chunkStart, chunkEnd),
            origin,
            shard.getOperationPrimaryTerm(),
            shard.getRelativeTimeInNanos()
        );
        final BatchMappingContext context = new BatchMappingContext(indexBatch, mappingLookup, shard.indexSettings());

        try {
            for (MetadataFieldMapper metadataMapper : metadataMappers) {
                metadataMapper.preColumnarParse(context);
            }
            // Invoke field mappers
            final SourceBatch sourceBatch = indexBatch.sourceBatch();
            if (sourceBatch instanceof EscfBatch escfChunk) {
                final FieldMapper[] columnMappers = resolution.columnMappers();
                for (int c = 0; c < columnMappers.length; c++) {
                    final FieldMapper mapper = columnMappers[c];
                    if (mapper != null) {
                        mapper.mapColumnBatch(context, escfChunk.column(c));
                    }
                }
                // Dispatch group mappers (e.g. flattened fields) after per-leaf mappers.
                for (ColumnGroupResolution group : resolution.columnGroups()) {
                    final int[] leafIndexes = group.leafIndexes();
                    final EscfColumn[] groupColumns = new EscfColumn[leafIndexes.length];
                    for (int i = 0; i < leafIndexes.length; i++) {
                        groupColumns[i] = escfChunk.column(leafIndexes[i]);
                    }
                    group.mapper().mapColumnGroupBatch(context, groupColumns, group.relativeKeys());
                }
            } else {
                throw new IllegalStateException("unexpected batch mapping - only use escf currently");
            }
            for (MetadataFieldMapper metadataMapper : metadataMappers) {
                metadataMapper.postColumnarParse(context);
            }
        } catch (Exception e) {
            logger.warn("columnar batch mapping failed on [{}], falling back", origin, e);
            return null;
        }

        return new EngineBatch(indexBatch, context.columns());
    }
}
