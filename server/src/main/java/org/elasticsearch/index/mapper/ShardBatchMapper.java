/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkItemRequest;
import org.elasticsearch.action.bulk.ShardBatchIndexer;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineBatch;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.mapper.ColumnGroupResolver.ColumnGroupLookup;
import org.elasticsearch.index.mapper.ColumnGroupResolver.ColumnGroupResolution;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.index.mapper.flattened.UnmappedSinkFieldMapper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.MeteringModel;
import org.elasticsearch.sourcebatch.SourceBatch;
import org.elasticsearch.sourcebatch.SourceSchema;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.IntPredicate;

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
 *     <li>{@link #mapColumnBatch(BulkItemRequest[], SourceBatch, BatchMappingShardContext, int, int, BatchMapperResolution,
 *     Engine.Operation.Origin, Recycler, DocumentParsingProvider)} runs per chunk. It meters the chunk under the provider's
 *     metering model, then invokes each mapper once for the whole chunk — attaching one Lucene column per batch-wide value
 *     (id, source, engine-assigned seq-no/version, ...) via {@link BatchMappingContext}, and assembles {@link Engine.Index}
 *     operations plus the resulting {@link EngineBatch}. After the per-leaf loop, each group mapper is dispatched via
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
    public record BatchMapperResolution(
        FieldMapper[] columnMappers,
        ColumnGroupResolution[] columnGroups,
        NestedGroupResolution[] nestedGroups
    ) {}

    /**
     * The schema leaves that belong to one {@code nested} mapper. Their values do not map to root columns: each array element (or
     * the object itself) becomes a child document. Strict columnar nests only one level, so a nested mapper is a direct child of the
     * root, and its leaves are either the leaf at its own path or leaves directly inside the object at its path.
     *
     * @param mapper the nested mapper
     * @param valueLeaves leaves whose full path is the nested path: the array spelling, {@code {}}, {@code null} and {@code []}
     * @param objectSpellingLeaves leaves directly inside the object at the nested path, the {@code {"n":{"b":1}}} spelling
     * @param relativeKeys the name of each object spelling leaf, which can contain dots; parallel to {@code objectSpellingLeaves}
     */
    public record NestedGroupResolution(NestedObjectMapper mapper, int[] valueLeaves, int[] objectSpellingLeaves, String[] relativeKeys) {}

    private static final NestedGroupResolution[] NO_NESTED_GROUPS = new NestedGroupResolution[0];

    /**
     * Resolve each schema leaf to a {@link FieldMapper}. Returns {@code null} if any scenario
     * falls outside the v1 batch-indexing support matrix and the caller should fall back to the
     * sequential path.
     */
    public static BatchMapperResolution resolveMappers(SourceBatch batch, MappingLookup lookup, IndexSettings indexSettings) {
        ColumnSupport columnSupport = batch instanceof EscfBatch escf
            ? (mapper, leaf) -> mapper.supportsColumnKind(escf.column(leaf).kind())
            : (mapper, leaf) -> true;
        return resolveMappers(batch.schema(), lookup, indexSettings, batch::isEmptyObjectColumn, columnSupport);
    }

    /**
     * Overload for tests that only have a schema. Column data is unavailable, so empty-object columns
     * cannot be detected and any unmapped leaf under a dynamic parent causes the usual fallback.
     */
    public static BatchMapperResolution resolveMappers(SourceSchema schema, MappingLookup lookup, IndexSettings indexSettings) {
        return resolveMappers(schema, lookup, indexSettings, leaf -> false, (mapper, leaf) -> true);
    }

    /**
     * Whether {@code mapper} takes the column of schema leaf {@code leaf}.
     */
    @FunctionalInterface
    private interface ColumnSupport {
        boolean test(FieldMapper mapper, int leaf);
    }

    private static BatchMapperResolution resolveMappers(
        SourceSchema schema,
        MappingLookup lookup,
        IndexSettings indexSettings,
        IntPredicate isEmptyObjectColumn,
        ColumnSupport columnSupport
    ) {
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
        // TODO: Implement
        if (lookup.getMapping().getMetadataMapperByName(IdFieldMapper.NAME) instanceof SliceIdFieldMapper) {
            logger.debug("batch indexing disabled: slice-enabled index");
            return null;
        }
        // Nested values become child documents, which only the strict columnar modes support: they nest one level deep and reject
        // copy_to. A nested mapper in any other mode, or one that copies its fields to the parent or root (older indices still accept
        // include_in_parent and include_in_root), needs the sequential path.
        final Map<String, NestedObjectMapper> nestedMappers = lookup.nestedLookup().getNestedMappers();
        if (nestedMappers.isEmpty() == false) {
            if (indexSettings.getMode().isStrictColumnar() == false) {
                logger.debug("batch indexing disabled: nested objects need a strict columnar index mode");
                return null;
            }
            for (NestedObjectMapper nested : nestedMappers.values()) {
                if (nested.isIncludeInParent() || nested.isIncludeInRoot()) {
                    logger.debug("batch indexing disabled: nested object [{}] has include_in_parent or include_in_root", nested.fullPath());
                    return null;
                }
            }
        }

        final MetadataFieldMapper[] metadataMappers = lookup.getMapping().getSortedMetadataMappers();

        // An empty metadata mapper array means the index has not yet received its first mapping — the
        // initial cluster-state mapping was null, so MapperService.mappingLookup() returned EMPTY. We
        // cannot determine columnar eligibility without metadata mappers (e.g. SourceFieldMapper governs
        // whether stored _source is compatible), so fall back to the sequential path.
        if (metadataMappers.length == 0) {
            logger.debug("batch indexing disabled: mapping not yet established (no metadata mappers)");
            return null;
        }

        for (MetadataFieldMapper mapper : metadataMappers) {
            if (mapper.supportsColumnarMetadataParse(indexSettings) == false) {
                logger.debug(
                    "columnar batch mapping disabled: metadata mapper of type [{}] does not support columnar parsing",
                    mapper.typeName()
                );
                return null;
            }
        }

        // Most mappings have no keep: all object, and then no leaf needs the walk up its parents.
        final boolean anyKeepAllObject = hasKeepAllObject(lookup, indexSettings);

        final int leafCount = schema.leafCount();
        final FieldMapper[] columnMappers = new FieldMapper[leafCount];
        ColumnGroupResolver.Builder groupBuilder = null;
        // Full paths of leaves bound to a per-leaf mapper, used to detect dotted/nested aliasing. Allocated lazily
        // because most batches resolve every leaf to a group or to nothing at all.
        HashSet<String> mappedPaths = null;
        // Owner path → the one schema object a group that needs a single source object takes its leaves from.
        HashMap<String, Integer> groupObjects = null;
        // Nested path → its leaves, ordered by path so that every node resolves the same batch the same way.
        TreeMap<String, NestedGroupBuilder> nestedBuilders = null;

        for (int leaf = 0; leaf < leafCount; leaf++) {
            final String fullPath = schema.getFullPath(leaf);
            if (hasBlankPathSegment(fullPath)) {
                logger.debug("batch indexing disabled: malformed field name [{}]", fullPath);
                return null;
            }
            // Checked for every leaf, whatever it resolves to, before any of the branches below that can accept it.
            if (anyKeepAllObject && isUnderKeepAllObject(fullPath, lookup, indexSettings)) {
                logger.debug("batch indexing disabled: leaf [{}] is under an object with synthetic_source_keep: all", fullPath);
                return null;
            }
            if (nestedMappers.isEmpty() == false) {
                final NestedLeaf nestedLeaf = claimNestedLeaf(schema, leaf, fullPath, nestedMappers);
                if (nestedLeaf == NestedLeaf.UNSUPPORTED) {
                    logger.debug("batch indexing disabled: leaf [{}] crosses the boundary of a nested object", fullPath);
                    return null;
                }
                if (nestedLeaf != null) {
                    if (nestedBuilders == null) {
                        nestedBuilders = new TreeMap<>();
                    }
                    nestedBuilders.computeIfAbsent(nestedLeaf.path(), k -> new NestedGroupBuilder(nestedMappers.get(k)))
                        .add(leaf, nestedLeaf);
                    // the value is shredded into child documents, not mapped as a root column
                    columnMappers[leaf] = null;
                    continue;
                }
            }
            final Mapper resolved = lookup.getMapper(fullPath);

            if (resolved == null) {
                // Before the runtime-field-shadow check: a leaf under a group mapper (e.g. flattened) has
                // no mapper of its own, but MappingLookup.getFieldType may still return non-null because
                // the owning mapper is a DynamicFieldType. The group check must precede the shadow check or
                // every flattened batch is incorrectly classified as containing a runtime-field shadow.
                final ColumnGroupLookup group = ColumnGroupResolver.findColumnGroup(fullPath, lookup);
                if (group instanceof ColumnGroupLookup.Conflict(FieldMapper mapper, String ownerPath)) {
                    // The document nests values beneath a leaf field. The sequential path reports this as a document parsing error or,
                    // with subobjects disabled, flattens the object into dotted fields. Fall back and let it do either rather than
                    // treating the leaf as unmapped, which could silently drop the value.
                    logger.debug(
                        "batch indexing disabled: leaf [{}] nests under the [{}] field mapper at [{}]",
                        fullPath,
                        mapper.typeName(),
                        ownerPath
                    );
                    return null;
                }
                if (group instanceof ColumnGroupLookup.Owned owned) {
                    // Whether the group mapper supports columnar parse is checked once all of its keys are known, below.
                    if (owned.mapper().resolvesColumnGroup() == FieldMapper.ColumnGroup.SINGLE_OBJECT) {
                        if (ColumnGroupResolver.isDirectlyInOwnerObject(schema, leaf, fullPath, owned.ownerPath()) == false) {
                            logger.debug(
                                "batch indexing disabled: leaf [{}] is not directly in an object at the [{}] group mapper's path [{}]",
                                fullPath,
                                owned.mapper().typeName(),
                                owned.ownerPath()
                            );
                            return null;
                        }
                        if (groupObjects == null) {
                            groupObjects = new HashMap<>();
                        }
                        final int parent = schema.getLeafParent(leaf);
                        final Integer groupObject = groupObjects.putIfAbsent(owned.ownerPath(), parent);
                        if (groupObject != null && groupObject != parent) {
                            logger.debug(
                                "batch indexing disabled: the [{}] group mapper at [{}] is spelled as more than one object in this batch",
                                owned.mapper().typeName(),
                                owned.ownerPath()
                            );
                            return null;
                        }
                    }
                    if (groupBuilder == null) {
                        groupBuilder = new ColumnGroupResolver.Builder();
                    }
                    groupBuilder.add(owned, leaf);
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
                    // In strict-columnar modes an enabled: false prefix resolves to dynamic: false too, and the sequential path can keep
                    // values here that the batch path cannot see. An array of objects is packed into one column named after the array, so
                    // {"obj":[{"field":1}]} arrives as the unmapped leaf [obj] and dropping it would lose a mapped obj.field. With
                    // subobjects disabled an object-matching dynamic template can also still map the value.
                    if (indexSettings.getMode().isStrictColumnar()) {
                        logger.debug("batch indexing disabled: unmapped leaf [{}] under dynamic=false", fullPath);
                        return null;
                    }
                    // The sequential path rejects an unmapped field that matches routing_path rather than dropping it,
                    // so fall back and let it raise the error with its own token location.
                    if (matchesRoutingPath(fullPath, indexSettings)) {
                        logger.debug("batch indexing disabled: unmapped leaf [{}] matches routing_path", fullPath);
                        return null;
                    }
                    // The sequential path copies the value of an unmapped leaf under dynamic: false into _ignored_source, when it
                    // records ignored source for this index at all, so that synthetic source still returns it. The batch path writes no
                    // such entry, so
                    // dropping the leaf here would lose the value; leave those batches to the sequential path. Where it does not
                    // capture (stored source), the value stays in _source and dropping the leaf is correct.
                    if (sequentialPathCapturesDynamicFalseLeaves(lookup, indexSettings)) {
                        logger.debug(
                            "batch indexing disabled: unmapped leaf [{}] under dynamic=false is kept in _ignored_source",
                            fullPath
                        );
                        return null;
                    }
                    columnMappers[leaf] = null;
                    continue;
                }
                if (parentDynamic == ObjectMapper.Dynamic.FLATTENED) {
                    final FieldMapper sink = unmappedSink(lookup, indexSettings);
                    if (sink == null) {
                        return null;
                    }
                    if (groupBuilder == null) {
                        groupBuilder = new ColumnGroupResolver.Builder();
                    }
                    // The sink is keyed by the leaf's full dotted path, matching UnmappedSinkFieldMapper.absorb,
                    // which calls indexValueAtPath with context.path().pathAsText(name).
                    groupBuilder.add(new ColumnGroupLookup.Owned(sink, FlattenedFieldMapper.UNMAPPED_SINK_NAME, fullPath), leaf);
                    columnMappers[leaf] = null;
                    continue;
                }
                // An empty-object column ({} in every present row) produces nothing in the sequential
                // path regardless of dynamic setting — DocumentParser finds no children and emits no
                // mapper, no value, and nothing to ignored source. Skip it rather than aborting.
                if (indexSettings.getMode().isStrictColumnar() && isEmptyObjectColumn.test(leaf)) {
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
            if (isMultiFieldSubField(fullPath, lookup)) {
                logger.debug("batch indexing disabled: field [{}] is a mapped as a multi-field", fullPath);
                return null;
            }
            if (fieldMapper.supportsColumnarParse(indexSettings, Set.of()) == false) {
                logger.debug(
                    "columnar batch mapping disabled: mapper at [{}] of type [{}] does not support columnar parsing",
                    fullPath,
                    fieldMapper.typeName()
                );
                return null;
            }
            if (columnSupport.test(fieldMapper, leaf) == false) {
                logger.debug("columnar batch mapping disabled: mapper at [{}] does not take this batch's column kind", fullPath);
                return null;
            }
            // Two schema leaves can share a full path when a batch mixes the dotted and nested spellings of the same
            // field ({"a.b":1} and {"a":{"b":1}}); the encoder keeps them as separate columns. Per-leaf mappers are
            // dispatched one column at a time, so a document carrying both spellings would emit two independent
            // outputs where the sequential path emits one merged multi-valued field. Group mappers are immune —
            // mapColumnGroupBatch receives all of a group's columns at once — so only the per-leaf columns are
            // checked here.
            if (mappedPaths == null) {
                mappedPaths = new HashSet<>(leafCount);
            }
            if (mappedPaths.add(fullPath) == false) {
                logger.debug("batch indexing disabled: [{}] is spelled both dotted and nested in this batch", fullPath);
                return null;
            }
            columnMappers[leaf] = fieldMapper;
        }

        final ColumnGroupResolution[] columnGroups = groupBuilder != null ? groupBuilder.build() : ColumnGroupResolver.EMPTY;
        for (ColumnGroupResolution columnGroup : columnGroups) {
            final FieldMapper mapper = columnGroup.mapper();
            // The unmapped sink is not a group mapper of its own; unmappedSink already checked that it can absorb leaves.
            if (mapper.resolvesColumnGroup() != FieldMapper.ColumnGroup.NONE
                && mapper.supportsColumnarParse(indexSettings, Set.copyOf(Arrays.asList(columnGroup.relativeKeys()))) == false) {
                logger.debug(
                    "columnar batch mapping disabled: group mapper at [{}] of type [{}] does not support columnar parsing of keys {}",
                    mapper.fullPath(),
                    mapper.typeName(),
                    columnGroup.relativeKeys()
                );
                return null;
            }
        }
        final NestedGroupResolution[] nestedGroups = nestedBuilders == null
            ? NO_NESTED_GROUPS
            : nestedBuilders.values().stream().map(NestedGroupBuilder::build).toArray(NestedGroupResolution[]::new);
        return new BatchMapperResolution(columnMappers, columnGroups, nestedGroups);
    }

    /** Where a schema leaf sits relative to a nested mapper, see {@link #claimNestedLeaf}. */
    private record NestedLeaf(String path, boolean objectSpelling, String relativeKey) {

        /** The leaf is under a nested path in a way that needs the sequential path. */
        static final NestedLeaf UNSUPPORTED = new NestedLeaf(null, false, null);
    }

    /**
     * Whether the nested mapper at {@code path} owns schema leaf {@code leaf}:
     * <ul>
     *     <li>The leaf's full path is the nested path: the array spelling, {@code {}}, {@code null} and {@code []}, and also the
     *     dotted value spelling {@code {"meta.comments":[...]}}, which the sequential path indexes the same way.</li>
     *     <li>The leaf sits directly inside the object at the nested path, so {@code {"n":{"b":1}}} gives the leaf {@code b} under
     *     the object {@code n}.</li>
     * </ul>
     * Any other leaf under the nested path, such as a dotted key across the boundary ({@code {"n.b":1}}) or a leaf in a deeper object,
     * is {@link NestedLeaf#UNSUPPORTED}: the sequential path trips an assertion on the first and makes extra child documents for the
     * second. Returns {@code null} for a leaf that is not under a nested path.
     */
    private static NestedLeaf claimNestedLeaf(SourceSchema schema, int leaf, String fullPath, Map<String, NestedObjectMapper> nested) {
        if (nested.containsKey(fullPath)) {
            return new NestedLeaf(fullPath, false, null);
        }
        // Strict columnar does not nest a nested mapper in another, so at most one prefix is a nested path.
        int dot = fullPath.lastIndexOf('.');
        while (dot > 0) {
            final String prefix = fullPath.substring(0, dot);
            if (nested.containsKey(prefix)) {
                return ColumnGroupResolver.isDirectlyInOwnerObject(schema, leaf, fullPath, prefix)
                    ? new NestedLeaf(prefix, true, schema.getLeafName(leaf))
                    : NestedLeaf.UNSUPPORTED;
            }
            dot = fullPath.lastIndexOf('.', dot - 1);
        }
        return null;
    }

    /** Accumulates the leaves of one nested mapper while {@link #resolveMappers} walks the schema. */
    private static final class NestedGroupBuilder {
        private final NestedObjectMapper mapper;
        private final List<Integer> valueLeaves = new ArrayList<>();
        private final List<Integer> objectSpellingLeaves = new ArrayList<>();
        private final List<String> relativeKeys = new ArrayList<>();

        NestedGroupBuilder(NestedObjectMapper mapper) {
            this.mapper = mapper;
        }

        void add(int leaf, NestedLeaf nestedLeaf) {
            if (nestedLeaf.objectSpelling()) {
                objectSpellingLeaves.add(leaf);
                relativeKeys.add(nestedLeaf.relativeKey());
            } else {
                valueLeaves.add(leaf);
            }
        }

        NestedGroupResolution build() {
            return new NestedGroupResolution(
                mapper,
                valueLeaves.stream().mapToInt(Integer::intValue).toArray(),
                objectSpellingLeaves.stream().mapToInt(Integer::intValue).toArray(),
                relativeKeys.toArray(String[]::new)
            );
        }
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
        //
        // The root fallback comes from getRootDynamic, the same value the sequential path seeds its root context with.
        final ObjectMapper.Dynamic rootFallback = ObjectMapper.Dynamic.getRootDynamic(lookup);
        return lookup.getMapping().getRoot().resolveDynamic(leafPath, rootFallback);
    }

    /**
     * Mirrors {@link DocumentParserContext#canAddIgnoredField()}, minus the per-document {@code recordedSource} part, which can
     * only make the sequential path capture less. When this is {@code true} the sequential path writes
     * {@code _ignored_source} for values it would otherwise drop; refusing slightly more than needed is always safe.
     */
    private static boolean sequentialPathCapturesDynamicFalseLeaves(MappingLookup lookup, IndexSettings indexSettings) {
        return lookup.isSourceSynthetic()
            && indexSettings.getSkipIgnoredSourceWrite() == false
            && indexSettings.getMode().isStrictColumnar() == false;
    }

    /**
     * Returns the implicit flattened {@code _unmapped} sink that absorbs unmapped leaves under
     * {@link ObjectMapper.Dynamic#FLATTENED}, or {@code null} if the batch cannot use it and must fall back.
     */
    private static FieldMapper unmappedSink(MappingLookup lookup, IndexSettings indexSettings) {
        // Dynamic templates are tried before the sink on the sequential path
        // (DynamicFieldsBuilder#createDynamicFieldFromValue), and a match creates a concrete field instead of
        // absorbing the value. The batch path cannot evaluate templates, so it cannot tell which leaves would
        // have been sunk.
        if (lookup.getMapping().getRoot().dynamicTemplates().length > 0) {
            logger.debug("batch indexing disabled: dynamic templates may pre-empt the [{}] sink", FlattenedFieldMapper.UNMAPPED_SINK_NAME);
            return null;
        }
        // Asked here rather than with the other metadata mappers, so that a sink that cannot batch only turns off batches that absorb.
        UnmappedSinkFieldMapper sink = lookup.getMapping().getMetadataMapperByClass(UnmappedSinkFieldMapper.class);
        if (sink.supportsColumnarAbsorb(indexSettings) == false) {
            logger.debug(
                "batch indexing disabled: the [{}] sink does not support columnar parsing",
                FlattenedFieldMapper.UNMAPPED_SINK_NAME
            );
            return null;
        }
        return sink;
    }

    /**
     * Whether any object in the mapping has {@code synthetic_source_keep: all}, which {@link #isUnderKeepAllObject} then has to look
     * for above each leaf. Strict-columnar modes reject the setting, and there the sequential path never writes
     * {@code _ignored_source} either, so no object counts.
     */
    private static boolean hasKeepAllObject(MappingLookup lookup, IndexSettings indexSettings) {
        if (indexSettings.getMode().isStrictColumnar()) {
            return false;
        }
        for (ObjectMapper object : lookup.objectMappers().values()) {
            if (keepsWholeObject(object, indexSettings)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Whether an enclosing object has {@code synthetic_source_keep: all}. The sequential path then copies the whole object into
     * {@code _ignored_source}, whatever its leaves' own mappers do, and the batch path writes no such entry. Call only when
     * {@link #hasKeepAllObject} is {@code true}.
     */
    private static boolean isUnderKeepAllObject(String leafPath, MappingLookup lookup, IndexSettings indexSettings) {
        String current = leafPath;
        int dot;
        while ((dot = current.lastIndexOf('.')) > 0) {
            current = current.substring(0, dot);
            final ObjectMapper parent = lookup.objectMappers().get(current);
            // The root mapping and the index setting both reject synthetic_source_keep: all, so only named objects can carry it.
            if (parent != null && keepsWholeObject(parent, indexSettings)) {
                return true;
            }
        }
        return false;
    }

    private static boolean keepsWholeObject(ObjectMapper object, IndexSettings indexSettings) {
        return FallbackPostMapper.capturesWholeObject(FallbackPostMapper.effectiveKeepMode(object.sourceKeepMode(), indexSettings));
    }

    /**
     * Detects whether a field is mapped as a multi-field. This to avoid trying to index a multi-field directly, which isn't allowed.
     * If a field is a multi-field, then {@link #resolveMappers(SourceSchema, MappingLookup, IndexSettings)} should
     * fall back to the sequential execution path, which will ignore the field in question. This is current behavior.
     */
    private static boolean isMultiFieldSubField(String fullPath, MappingLookup lookup) {
        int dot = fullPath.lastIndexOf('.');
        if (dot <= 0) {
            return false;
        }
        // Only check immediate dotted parent, because multi-fields are always leaf fields:
        if (lookup.getMapper(fullPath.substring(0, dot)) instanceof FieldMapper ancestor) {
            for (FieldMapper subMapper : ancestor.multiFields()) {
                if (subMapper.fullPath().equals(fullPath)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns {@code true} if an unmapped field at {@code fullPath} matches {@code routing_path}. The sequential
     * path rejects the document in that case rather than dropping the field
     * ({@code DocumentParser#failIfMatchesRoutingPath}).
     */
    private static boolean matchesRoutingPath(String fullPath, IndexSettings indexSettings) {
        final List<String> routingPaths = indexSettings.getIndexMetadata().getRoutingPaths();
        return routingPaths.isEmpty() == false && Regex.simpleMatch(routingPaths, fullPath);
    }

    /**
     * Returns {@code true} if {@code fullPath} is empty, or contains a dot and any dot-separated segment is blank.
     *
     * <p>Mirrors the field-name validation {@link DotExpandingXContentParser} applies on the sequential path,
     * which the columnar encoder does not perform — {@link SourceSchema#getFullPath} joins names verbatim. Such a
     * name would otherwise resolve to nothing here and be silently dropped under a {@code dynamic: false} prefix
     * while the sequential path rejects it. Falling back is deliberately slightly over-eager: a trailing dot
     * ({@code "a."}) is accepted there as plain {@code "a"} but rejected here, which costs a fallback rather than
     * correctness.
     */
    private static boolean hasBlankPathSegment(String fullPath) {
        if (fullPath.isEmpty()) {
            return true;
        }
        if (fullPath.indexOf('.') < 0) {
            // A dotless name is never expanded, so the sequential path does not validate it either.
            return false;
        }
        int start = 0;
        int dot;
        while ((dot = fullPath.indexOf('.', start)) >= 0) {
            if (fullPath.substring(start, dot).isBlank()) {
                return true;
            }
            start = dot + 1;
        }
        return fullPath.substring(start).isBlank();
    }

    /**
     * Executes the columnar batch-mapping fast path for one chunk. Returns {@code null} (the
     * fallback signal — same contract as {@link #resolveMappers}) if mapping hits an unexpected
     * exception.
     */
    public static EngineBatch mapColumnBatch(
        BulkItemRequest[] items,
        SourceBatch batch,
        BatchMappingShardContext shard,
        int chunkStart,
        int chunkEnd,
        BatchMapperResolution resolution,
        Engine.Operation.Origin origin,
        Recycler<BytesRef> recycler,
        DocumentParsingProvider documentParsingProvider
    ) {
        final MappingLookup mappingLookup = shard.mapperService().mappingLookup();
        final MetadataFieldMapper[] metadataMappers = mappingLookup.getMapping().getSortedMetadataMappers();

        final SourceBatch chunkSource = batch.slice(chunkStart, chunkEnd);
        // TODO: the nested values of this chunk are not mapped to child documents yet.
        if (hasNestedValues(resolution.nestedGroups(), chunkSource)) {
            logger.debug("columnar batch mapping disabled: the chunk has values for nested objects");
            return null;
        }
        final long[] normalizedSizes;
        try {
            normalizedSizes = meterRows(documentParsingProvider, chunkSource);
        } catch (Exception e) {
            logger.warn("metering columnar batch failed on [{}], falling back", origin, e);
            return null;
        }
        final IndexOperationBatch indexBatch = IndexOperationBatch.initFromBulk(
            items,
            chunkStart,
            chunkEnd,
            chunkSource,
            shard.indexSettings().getMode(),
            origin,
            shard.getOperationPrimaryTerm(),
            shard.getRelativeTimeInNanos(),
            normalizedSizes
        );
        final BatchMappingContext context = new BatchMappingContext(indexBatch, mappingLookup, shard.indexSettings(), recycler);

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
                enforceRequiredFields(context, mappingLookup, escfChunk.schema(), "", chunkEnd - chunkStart);
            } else {
                throw new IllegalStateException("unexpected batch mapping - only use escf currently");
            }
            for (MetadataFieldMapper metadataMapper : metadataMappers) {
                metadataMapper.postColumnarParse(context);
            }
            // the engine's DocumentSizeReporter sees the metered sizes while columns can still be attached; a no-op without a reporter
            shard.onBatchParsingCompleted(context);
        } catch (Exception e) {
            logger.warn("columnar batch mapping failed on [{}], falling back", origin, e);
            context.close();
            return null;
        }

        return new EngineBatch(indexBatch, context.columns(), context);
    }

    /** Whether any row of {@code chunk} has a value, also {@code null}, for a leaf that a nested mapper owns. */
    private static boolean hasNestedValues(NestedGroupResolution[] nestedGroups, SourceBatch chunk) {
        if (nestedGroups.length == 0) {
            return false;
        }
        if (chunk instanceof EscfBatch == false) {
            return true;
        }
        final EscfBatch escf = (EscfBatch) chunk;
        for (NestedGroupResolution group : nestedGroups) {
            for (int leaf : group.valueLeaves()) {
                if (escf.column(leaf).presentDocs().nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    return true;
                }
            }
            for (int leaf : group.objectSpellingLeaves()) {
                if (escf.column(leaf).presentDocs().nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Enforces {@code nullability: false} for the fields of {@code nestedParent} ({@code ""} for the root) that are absent from the
     * ESCF schema. Fields that are in the schema but carry null or absent values are checked by {@link FieldMapper#mapColumnBatch};
     * this covers the remaining case where the field produced no column at all because every document in the batch omitted it.
     */
    private static void enforceRequiredFields(
        BatchMappingContext context,
        MappingLookup mappingLookup,
        SourceSchema schema,
        String nestedParent,
        int docCount
    ) {
        if (mappingLookup.hasRequiredFields() == false) {
            return;
        }
        final HashSet<String> schemaLeafPaths = new HashSet<>(schema.leafCount() * 2);
        for (int c = 0; c < schema.leafCount(); c++) {
            schemaLeafPaths.add(schema.getFullPath(c));
        }
        for (String fieldName : mappingLookup.requiredFields(nestedParent)) {
            // A multi-field has no column of its own: it is mapped from its parent's, so it is present whenever the parent is.
            final String sourcePath = isMultiFieldSubField(fieldName, mappingLookup)
                ? fieldName.substring(0, fieldName.lastIndexOf('.'))
                : fieldName;
            if (schemaLeafPaths.contains(sourcePath) == false) {
                final Mapper m = mappingLookup.getMapper(fieldName);
                assert m instanceof FieldMapper : "required field [" + fieldName + "] must be a FieldMapper";
                final FieldMapper fieldMapper = (FieldMapper) m;
                if (fieldMapper.onFailureBehavior() == FieldMapper.DocValuesParameter.Values.OnFailure.IGNORE) {
                    for (int doc = 0; doc < docCount; doc++) {
                        context.addIgnoredFieldColumnar(doc, fieldName);
                    }
                } else {
                    throw new UnsupportedOperationException(
                        "mapColumnBatch: nullability=false field [" + fieldName + "] is absent from the ESCF schema"
                    );
                }
            }
        }
    }

    /**
     * Meters every row of {@code chunkSource} the way {@link DocumentParser} meters a row-parsed document, in one sweep over the
     * batch's columns under the provider's {@link MeteringModel}. The model charges values only and excludes whole root
     * fields. Returns {@code null} when documents are not metered.
     */
    @Nullable
    private static long[] meterRows(DocumentParsingProvider documentParsingProvider, SourceBatch chunkSource) {
        assert chunkSource instanceof EscfBatch : "Only ESCF batches are supported for metering on batch indexing";
        final MeteringModel model = documentParsingProvider.newMeteringModel();
        return model == null ? null : chunkSource.meteredRowSizes(model);
    }
}
