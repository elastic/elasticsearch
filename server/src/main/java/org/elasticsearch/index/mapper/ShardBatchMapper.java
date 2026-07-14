/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;

/**
 * Batch-time mapper resolution and columnar batch mapping for the bulk batch-indexing fast path.
 *
 * <p>Workflow:
 * <ol>
 *     <li>{@link #resolveMappers(SourceSchema, MappingLookup)} runs once per batch. It walks the
 *     schema leaves and binds each column to a {@link FieldMapper} (or records {@code null} for
 *     columns that are silently ignored under a {@code dynamic=false} parent). Any configuration
 *     outside the v1 support matrix — runtime fields, index-time scripts, dynamic mapping,
 *     unsupported mapper types, etc. — causes the method to return {@code null}, at which point
 *     {@link ShardBatchIndexer} falls back to the sequential path.</li>
 *     <li>{@link #mapColumnBatch(BulkItemRequest[], SourceBatch, IndexShard, int, int, BatchMapperResolution, Engine.Operation.Origin)}
 *     runs per chunk. It invokes each metadata mapper once for the whole chunk — see
 *     {@link MetadataFieldMapper#preColumnarParse} / {@link MetadataFieldMapper#postColumnarParse} —
 *     attaching one Lucene column per batch-wide value (id, source, engine-assigned seq-no/version,
 *     ...) via {@link BatchMappingContext}, and assembles {@link Engine.Index} operations plus the
 *     resulting {@link EngineBatch}.</li>
 * </ol>
 *
 * <p><b>First-pass scope:</b> only metadata mappers support columnar parsing so far (see
 * {@link FieldMapper#supportsColumnarParse(IndexSettings)} overrides). Field (non-metadata) mappers do not yet,
 * so {@link #mapColumnBatch} only fully engages for chunks whose schema has no leaves at all (every
 * document in the chunk has an empty {@code {}} body) — any chunk with real field data falls back to
 * the sequential path, same as an unsupported mapper or dynamic mapping update.
 */
public final class ShardBatchMapper {

    private static final Logger logger = LogManager.getLogger(ShardBatchMapper.class);

    private ShardBatchMapper() {}

    /**
     * Result of {@link #resolveMappers(SourceSchema, MappingLookup)}. Holds one entry per schema
     * leaf; a {@code null} entry means the column is silently ignored because its nearest
     * existing parent {@link ObjectMapper} has {@code dynamic=false}.
     */
    public record BatchMapperResolution(FieldMapper[] columnMappers) {}

    /**
     * Resolve each schema leaf to a {@link FieldMapper}. Returns {@code null} if any scenario
     * falls outside the v1 batch-indexing support matrix and the caller should fall back to the
     * sequential path.
     */
    public static BatchMapperResolution resolveMappers(SourceSchema schema, MappingLookup lookup) {
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

        final int leafCount = schema.leafCount();
        final FieldMapper[] columnMappers = new FieldMapper[leafCount];

        for (int leaf = 0; leaf < leafCount; leaf++) {
            final String fullPath = schema.getFullPath(leaf);
            final Mapper resolved = lookup.getMapper(fullPath);

            if (resolved == null) {
                // A field type without a mapper indicates a runtime field shadow.
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

            if (fieldMapper.supportsBatchIndexing() == false) {
                logger.info(
                    "batch indexing disabled: mapper at [{}] of type [{}] does not support batch indexing",
                    fullPath,
                    fieldMapper.typeName()
                );
                return null;
            }

            columnMappers[leaf] = fieldMapper;
        }

        return new BatchMapperResolution(columnMappers);
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
        final ObjectMapper.Dynamic rootDynamic = lookup.getMapping().getRoot().dynamic();
        return rootDynamic == null ? ObjectMapper.Dynamic.TRUE : rootDynamic;
    }

    /**
     * Executes the columnar batch-mapping fast path for one chunk. Returns {@code null} (the
     * fallback signal — same contract as {@link #resolveMappers}) if any resolved field mapper or
     * any sorted metadata mapper does not support columnar parsing, or if mapping hits an
     * unexpected exception.
     *
     * <p>When {@code origin} is {@link Engine.Operation.Origin#PRIMARY}, each {@link Engine.Index}
     * operation is built with {@code UNASSIGNED_SEQ_NO} and version/versionType from the request,
     * leaving seq-no and version assignment to the engine. When {@code origin} is
     * {@link Engine.Operation.Origin#REPLICA}, the pre-assigned values from the primary response
     * ({@code _seq_no}, {@code _primary_term}, {@code _version}) are used instead; the caller
     * must ensure every item in {@code [chunkStart, chunkEnd)} has a successful primary response.
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
        final IndexSettings indexSettings = shard.indexSettings();
        for (FieldMapper mapper : resolution.columnMappers()) {
            if (mapper != null && mapper.supportsColumnarParse(indexSettings) == false) {
                logger.debug("columnar batch mapping disabled: mapper of type [{}] does not support columnar parsing", mapper.typeName());
                return null;
            }
        }

        final MappingLookup mappingLookup = shard.mapperService().mappingLookup();
        final MetadataFieldMapper[] metadataMappers = mappingLookup.getMapping().getSortedMetadataMappers();
        for (MetadataFieldMapper mapper : metadataMappers) {
            if (mapper.supportsColumnarParse(indexSettings) == false) {
                logger.debug(
                    "columnar batch mapping disabled: metadata mapper of type [{}] does not support columnar parsing",
                    mapper.typeName()
                );
                return null;
            }
        }

        final int docCount = chunkEnd - chunkStart;
        final IndexRequest[] requests = new IndexRequest[docCount];
        for (int d = 0; d < docCount; d++) {
            requests[d] = (IndexRequest) items[chunkStart + d].request();
        }
        final BatchMappingContext context = new BatchMappingContext(requests, mappingLookup, indexSettings);

        try {
            for (MetadataFieldMapper metadataMapper : metadataMappers) {
                metadataMapper.preColumnarParse(context);
            }
            // No field (non-metadata) mappers run yet — see class javadoc. Once one supports
            // columnar parsing it is invoked here, once per batch, over resolution.columnMappers().
            for (MetadataFieldMapper metadataMapper : metadataMappers) {
                metadataMapper.postColumnarParse(context);
            }
        } catch (Exception e) {
            logger.warn("columnar batch mapping failed on [{}], falling back", origin, e);
            return null;
        }

        final List<Engine.Index> operations = new ArrayList<>(docCount);
        // Placeholder: the real _seq_no/_primary_term/_version values live in the columns the
        // engine fills post-mapping (see BatchMappingContext#seqNoArray et al.); this LuceneDocument
        // is otherwise empty this pass since no field mapper has added anything to it.
        final SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID(
            shard.indexSettings().seqNoIndexOptions()
        );
        // Uid-encoded ids were already computed once by the id mapper's preColumnarParse.
        final BytesRef[] encodedIds = context.uids();
        for (int d = 0; d < docCount; d++) {
            final IndexRequest request = requests[d];
            final XContentType xContentType = request.getContentType() != null ? request.getContentType() : XContentType.JSON;
            final ParsedDocument parsedDoc = new ParsedDocument(
                VersionFieldMapper.versionField(),
                seqID,
                request.id(),
                request.routing(),
                List.of(new LuceneDocument()),
                request.source(),
                xContentType,
                null,
                XContentMeteringParserDecorator.UNKNOWN_SIZE
            );

            final long seqNo;
            final long primaryTerm;
            final long version;
            final VersionType versionType;
            final long ifSeqNo;
            final long ifPrimaryTerm;
            if (origin == Engine.Operation.Origin.REPLICA) {
                final DocWriteResponse resp = items[chunkStart + d].getPrimaryResponse().getResponse();
                seqNo = resp.getSeqNo();
                primaryTerm = resp.getPrimaryTerm();
                version = resp.getVersion();
                versionType = null;
                ifSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
                ifPrimaryTerm = 0;
            } else {
                seqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
                primaryTerm = shard.getOperationPrimaryTerm();
                version = request.version();
                versionType = request.versionType();
                ifSeqNo = request.ifSeqNo();
                ifPrimaryTerm = request.ifPrimaryTerm();
            }

            operations.add(
                new Engine.Index(
                    encodedIds[d],
                    parsedDoc,
                    seqNo,
                    primaryTerm,
                    version,
                    versionType,
                    origin,
                    shard.getRelativeTimeInNanos(),
                    request.getAutoGeneratedTimestamp(),
                    request.isRetry(),
                    ifSeqNo,
                    ifPrimaryTerm
                )
            );
        }

        return new EngineBatch(operations, batch.slice(chunkStart, chunkEnd), context.columns());
    }
}
