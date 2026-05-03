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
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfRowReader;
import org.elasticsearch.eirf.EirfRowToXContent;
import org.elasticsearch.eirf.EirfRowXContentParser;
import org.elasticsearch.eirf.EirfSchema;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.internal.XContentMeteringParserDecorator;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Batch-time mapper resolution and per-row field parsing for the bulk batch-indexing fast path.
 *
 * <p>Workflow:
 * <ol>
 *     <li>{@link #resolveMappers(EirfSchema, MappingLookup)} runs once per batch. It walks the
 *     schema leaves and binds each column to a {@link FieldMapper} (or records {@code null} for
 *     columns that are silently ignored under a {@code dynamic=false} parent). Any configuration
 *     outside the v1 support matrix — runtime fields, index-time scripts, dynamic mapping,
 *     unsupported mapper types, etc. — causes the method to return {@code null}, at which point
 *     {@link ShardBatchIndexer} falls back to the sequential path.</li>
 *     <li>{@link #parseMappings(BulkItemRequest[], EirfBatch, IndexShard, int, int, BatchMapperResolution)}
 *     runs per chunk. For each row it drives the pre-resolved mappers through their
 *     {@link FieldMapper#parse(org.elasticsearch.index.mapper.DocumentParserContext)} entry point
 *     using {@link EirfRowXContentParser#positionAtLeafValue(int)} as the value source, and
 *     assembles {@link Engine.Index} operations.</li>
 * </ol>
 */
public final class ShardBatchMapper {

    private static final Logger logger = LogManager.getLogger(ShardBatchMapper.class);

    private ShardBatchMapper() {}

    /**
     * Result of {@link #resolveMappers(EirfSchema, MappingLookup)}. Holds one entry per schema
     * leaf; a {@code null} entry means the column is silently ignored because its nearest
     * existing parent {@link ObjectMapper} has {@code dynamic=false}.
     */
    public record BatchMapperResolution(FieldMapper[] columnMappers) {}

    /**
     * Resolve each schema leaf to a {@link FieldMapper}. Returns {@code null} if any scenario
     * falls outside the v1 batch-indexing support matrix and the caller should fall back to the
     * sequential path.
     */
    public static BatchMapperResolution resolveMappers(EirfSchema schema, MappingLookup lookup) {
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
                logger.debug(
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
     * Parse one chunk of rows into {@link Engine.Index} operations, driving each pre-resolved
     * mapper through its normal {@link FieldMapper#parse} entry point with an
     * {@link EirfRowXContentParser} positioned at the leaf's value. Returns {@code null} if any
     * unexpected condition is hit; the caller will then fall back to the sequential path.
     */
    public static List<Engine.Index> parseMappings(
        BulkItemRequest[] items,
        EirfBatch batch,
        IndexShard primary,
        int chunkEnd,
        int chunkStart,
        BatchMapperResolution resolution
    ) throws IOException {
        final List<Engine.Index> operations = new ArrayList<>(chunkEnd - chunkStart);
        final EirfSchema schema = batch.schema();
        final MappingLookup mappingLookup = primary.mapperService().mappingLookup();
        final MetadataFieldMapper[] metadataMappers = mappingLookup.getMapping().getSortedMetadataMappers();
        // The schema tree is required by the EirfRowXContentParser constructor but is not used
        // along the per-leaf positioning path; built once per chunk.
        final EirfRowXContentParser.SchemaNode schemaTree = EirfRowXContentParser.buildSchemaTree(schema);
        final FieldMapper[] columnMappers = resolution.columnMappers();

        for (int i = chunkStart; i < chunkEnd; i++) {
            final IndexRequest indexRequest = (IndexRequest) items[i].request();
            final EirfRowReader row = batch.getRowReader(i);
            final EirfRowXContentParser rowParser = new EirfRowXContentParser(schemaTree, row);

            final XContentType xContentType = indexRequest.getContentType() != null ? indexRequest.getContentType() : XContentType.JSON;
            // TODO: Right now we materialize a source back to avoid breaking translog assertions. We should fix the translog assertions
            // and move to just materializing the original x-content source for stored source mapping
            final BytesReference source = rowToSource(row, schema, xContentType);
            // TODO: Metering and getIncludeSourceOnError currently do not work with EIRF parsing
            final SourceToParse sourceToParse = new SourceToParse(
                indexRequest.id(),
                source,
                xContentType,
                indexRequest.routing(),
                indexRequest.getDynamicTemplates(),
                indexRequest.getDynamicTemplateParams(),
                indexRequest.getIncludeSourceOnError(),
                XContentMeteringParserDecorator.NOOP,
                indexRequest.tsid()
            );

            final ParsedDocument parsedDoc;
            try {
                parsedDoc = parseRow(
                    sourceToParse,
                    mappingLookup,
                    metadataMappers,
                    rowParser,
                    columnMappers,
                    primary.mapperService().parserContext()
                );
            } catch (Exception e) {
                logger.warn("batch indexing on primary failed to parse row [{}], falling back", i, e);
                return null;
            }
            if (parsedDoc.dynamicMappingsUpdate() != null) {
                // Should not happen given resolve-time guards; defense in depth.
                logger.debug("batch indexing on primary encountered unexpected dynamic mapping update at item [{}], falling back", i);
                return null;
            }

            operations.add(
                new Engine.Index(
                    Uid.encodeId(parsedDoc.id()),
                    parsedDoc,
                    SequenceNumbers.UNASSIGNED_SEQ_NO,
                    primary.getOperationPrimaryTerm(),
                    indexRequest.version(),
                    indexRequest.versionType(),
                    Engine.Operation.Origin.PRIMARY,
                    primary.getRelativeTimeInNanos(),
                    indexRequest.getAutoGeneratedTimestamp(),
                    indexRequest.isRetry(),
                    indexRequest.ifSeqNo(),
                    indexRequest.ifPrimaryTerm()
                )
            );
        }
        return operations;
    }

    private static ParsedDocument parseRow(
        SourceToParse sourceToParse,
        MappingLookup mappingLookup,
        MetadataFieldMapper[] metadataMappers,
        EirfRowXContentParser rowParser,
        FieldMapper[] columnMappers,
        MappingParserContext mappingParserContext
    ) throws IOException {
        final BatchDocumentParserContext ctx = new BatchDocumentParserContext(mappingLookup, mappingParserContext, sourceToParse);

        for (MetadataFieldMapper metadataMapper : metadataMappers) {
            metadataMapper.preParse(ctx);
        }

        for (int leaf = 0; leaf < columnMappers.length; leaf++) {
            final FieldMapper mapper = columnMappers[leaf];
            if (mapper == null) {
                continue;
            }
            rowParser.positionAtLeafValue(leaf);
            ctx.setParser(rowParser);
            mapper.parse(ctx);
        }
        ctx.setParser(null);

        for (MetadataFieldMapper metadataMapper : metadataMappers) {
            metadataMapper.postParse(ctx);
        }

        final LuceneDocument doc = ctx.rootDoc();
        return new ParsedDocument(
            ctx.version(),
            ctx.seqID(),
            ctx.id(),
            sourceToParse.routing(),
            List.of(doc),
            sourceToParse.source(),
            sourceToParse.getXContentType(),
            null,
            XContentMeteringParserDecorator.UNKNOWN_SIZE
        );
    }

    private static BytesReference rowToSource(EirfRowReader row, EirfSchema schema, XContentType xContentType) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            EirfRowToXContent.writeRow(row, schema, builder);
            return BytesReference.bytes(builder);
        }
    }
}
