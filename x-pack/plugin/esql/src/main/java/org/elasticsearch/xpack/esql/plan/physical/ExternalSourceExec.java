/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;
import org.elasticsearch.xpack.esql.datasources.ExternalSchema;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Generic physical plan node for reading from external data sources (e.g., Iceberg tables, Parquet files), one row per
 * file row. This is the unified physical plan node for all external sources, replacing source-specific nodes; what it
 * holds, and which of it is serialized, is described on {@link AbstractExternalSourceExec}. A scan that reads a
 * collection at element grain is its sibling {@link ExpandingExternalSourceExec}, made by
 * {@link #withExpansion(List, ExpandingExternalSourceExec.Expansion)}.
 */
public class ExternalSourceExec extends AbstractExternalSourceExec<ExternalSourceExec> {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "ExternalSourceExec",
        ExternalSourceExec::readFrom
    );

    /**
     * Public 13-arg ctor used by tree tests.
     * Passes {@code null} for {@code unifiedSchema}; callers that need to carry the Unified schema
     * (e.g. {@link org.elasticsearch.xpack.esql.plan.logical.ExternalRelation#toPhysicalExec})
     * apply it afterwards via {@link #withUnifiedSchema(ExternalSchema)}.
     */
    public ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        List<Expression> pushedExpressions,
        int pushedLimit,
        Integer estimatedRowSize,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        List<ExternalSplit> splits
    ) {
        this(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            null,
            estimatedRowSize,
            fileList,
            schemaMap,
            null,
            splits
        );
    }

    /**
     * Public 15-arg ctor used by {@link #info()} (via constructor reference) and by tree tests: the 13-arg shape above
     * plus {@code datasetName} and {@code declaredReadSpec}, so node-reflection reconstruction preserves both. Losing
     * {@code datasetName} on a generic rewrite would drop it from the wire, and a 9.5 data node reads that slot to
     * compose {@code _index}; losing {@code declaredReadSpec} would
     * silently drop the declared renames (the same reflection safety they had while riding the
     * reflected {@code config} map). This is the longest public ctor — {@code EsqlNodeSubclassTests} keys the required
     * {@link #info()} arity off it. Passes {@code null} for {@code pushedTopN} / {@code unifiedSchema}; those are
     * transient hints carried via their {@code with*} methods.
     */
    public ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        List<Expression> pushedExpressions,
        int pushedLimit,
        Integer estimatedRowSize,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        List<ExternalSplit> splits,
        @Nullable String datasetName,
        DeclaredReadSpec declaredReadSpec
    ) {
        this(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            null,
            estimatedRowSize,
            fileList,
            schemaMap,
            null,
            splits,
            datasetName,
            false,
            declaredReadSpec
        );
    }

    /**
     * Primary constructor that also accepts the transient {@link BlockHash.TopNDef} hint for in-hash TopN pruning
     * and the coordinator-only {@link ExternalSchema} that carries the pre-prune Unified schema. Package-private on purpose
     * so the public, longest constructor (used by tooling and tree tests — {@code EsqlNodeSubclassTests} keys the info()
     * arity off it) remains the {@code declaredReadSpec}-carrying one below, not this transient-hint shape.
     * Use {@link #withPushedTopN(BlockHash.TopNDef)} and {@link #withUnifiedSchema(ExternalSchema)} from outside the package.
     */
    ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        List<Expression> pushedExpressions,
        int pushedLimit,
        @Nullable BlockHash.TopNDef pushedTopN,
        Integer estimatedRowSize,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        @Nullable ExternalSchema unifiedSchema,
        List<ExternalSplit> splits
    ) {
        this(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            pushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            splits,
            null,
            false,
            DeclaredReadSpec.NONE
        );
    }

    ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        List<Expression> pushedExpressions,
        int pushedLimit,
        @Nullable BlockHash.TopNDef pushedTopN,
        Integer estimatedRowSize,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        @Nullable ExternalSchema unifiedSchema,
        List<ExternalSplit> splits,
        @Nullable String datasetName,
        boolean deferredExtraction,
        DeclaredReadSpec declaredReadSpec
    ) {
        super(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            pushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            splits,
            datasetName,
            deferredExtraction,
            declaredReadSpec
        );
    }

    /**
     * Convenience ctor (no splits/fileList/schema). Use {@link #withFileList(FileList)},
     * {@link #withSplits(List)}, and the other {@code with*} builders to attach the build-time and
     * pushdown state. For tests: a {@code FROM <dataset>} plan builds its scan through
     * {@code ExternalRelation.toPhysicalExec}.
     */
    public ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        Integer estimatedRowSize
    ) {
        this(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            List.of(),
            FormatReader.NO_LIMIT,
            estimatedRowSize,
            null,
            Map.of(),
            List.of()
        );
    }

    /** Convenience ctor with no pushed filter; see {@link #ExternalSourceExec(Source, String, String, List, Map, Map, Object, Integer)}. */
    public ExternalSourceExec(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Integer estimatedRowSize
    ) {
        this(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            null,
            List.of(),
            FormatReader.NO_LIMIT,
            estimatedRowSize,
            null,
            Map.of(),
            List.of()
        );
    }

    private static ExternalSourceExec readFrom(StreamInput in) throws IOException {
        var source = Source.readFrom((PlanStreamInput) in);
        String sourcePath = in.readString();
        String sourceType = in.readString();
        var attributes = in.readNamedWriteableCollectionAsList(Attribute.class);
        @SuppressWarnings("unchecked")
        Map<String, Object> config = (Map<String, Object>) in.readGenericValue();
        @SuppressWarnings("unchecked")
        Map<String, Object> sourceMetadata = (Map<String, Object>) in.readGenericValue();
        Integer estimatedRowSize = in.readOptionalVInt();
        List<ExternalSplit> splits = in.getTransportVersion().supports(ESQL_EXTERNAL_SOURCE_SPLITS)
            ? in.readNamedWriteableCollectionAsList(ExternalSplit.class)
            : List.of();
        String datasetName = in.getTransportVersion().supports(ESQL_EXTERNAL_DATASET_NAME) ? in.readOptionalString() : null;
        DeclaredReadSpec declaredReadSpec = in.getTransportVersion().supports(DATASET_DECLARED_SCHEMA)
            ? DeclaredReadSpec.readFrom(in)
            : DeclaredReadSpec.NONE;

        return new ExternalSourceExec(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            null,
            List.of(),
            FormatReader.NO_LIMIT,
            null,
            estimatedRowSize,
            null,
            Map.of(),
            null,
            splits,
            datasetName,
            false,
            declaredReadSpec
        );
    }

    @Override
    protected ExternalSourceExec copy(
        Source source,
        String sourcePath,
        String sourceType,
        List<Attribute> attributes,
        Map<String, Object> config,
        Map<String, Object> sourceMetadata,
        Object pushedFilter,
        List<Expression> pushedExpressions,
        int pushedLimit,
        @Nullable BlockHash.TopNDef pushedTopN,
        Integer estimatedRowSize,
        FileList fileList,
        Map<StoragePath, SchemaReconciliation.FileSchemaInfo> schemaMap,
        @Nullable ExternalSchema unifiedSchema,
        List<ExternalSplit> splits,
        @Nullable String datasetName,
        boolean deferredExtraction,
        DeclaredReadSpec declaredReadSpec
    ) {
        return new ExternalSourceExec(
            source,
            sourcePath,
            sourceType,
            attributes,
            config,
            sourceMetadata,
            pushedFilter,
            pushedExpressions,
            pushedLimit,
            pushedTopN,
            estimatedRowSize,
            fileList,
            schemaMap,
            unifiedSchema,
            splits,
            datasetName,
            deferredExtraction,
            declaredReadSpec
        );
    }

    /**
     * This scan reading, besides its own columns, the collection {@code expansion} names at element grain, every other
     * field kept; see {@link ExpandingExternalSourceExec}.
     */
    public ExpandingExternalSourceExec withExpansion(List<Attribute> expanded, ExpandingExternalSourceExec.Expansion expansion) {
        return new ExpandingExternalSourceExec(
            source(),
            sourcePath(),
            sourceType(),
            attributes(),
            config(),
            sourceMetadata(),
            pushedFilter(),
            pushedExpressions(),
            pushedLimit(),
            pushedTopN(),
            estimatedRowSize(),
            fileList(),
            schemaMap(),
            unifiedSchema(),
            splits(),
            datasetName(),
            deferredExtraction(),
            declaredReadSpec(),
            expanded,
            expansion
        );
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        // pushedTopN: excluded — transient local-execution hint; including it would break the
        // node-reflection invariant in EsqlNodeSubclassTests#testInfoParameters. Preserved via
        // explicit with* methods; rendered in nodeString() for debuggability.
        // unifiedSchema: also excluded — the optimizer's attribute-rewriting rules walk every arg
        // in info() and would prune the Unified schema along with `attributes`, defeating its
        // whole purpose. Preserved through with* methods which carry it explicitly.
        // datasetName: INCLUDED — it is a plain String (attribute rewriting cannot prune it) and the
        // executing node rebinds config and sourcePath from the definition it names
        // (DatasetConfigResolver); a 9.5 data node also reads it to compose _index during a rolling
        // upgrade. Excluding it would drop the slot whenever a generic rule reconstructs this node via
        // node reflection. _name itself is folded on the logical relation by
        // MaterializeRelationClassAndName, and _index is a null constant here.
        // Mirrors ExternalRelation#info.
        // declaredReadSpec: INCLUDED for the same reason — it carries no Attributes (attribute
        // rewriting cannot prune it) and holds the declared renames; the renames used
        // to ride the reflected `config` map, so dropping them on a generic reflection rebuild
        // would be a silent regression. Mirrors datasetName.
        // deferredExtraction: excluded — transient local-execution signal like pushedTopN, set by
        // InsertExternalFieldExtraction after every reflection-driven rewrite has run; preserved
        // via withDeferredExtraction().
        return NodeInfo.create(
            this,
            ExternalSourceExec::new,
            sourcePath(),
            sourceType(),
            attributes(),
            config(),
            sourceMetadata(),
            pushedFilter(),
            pushedExpressions(),
            pushedLimit(),
            estimatedRowSize(),
            fileList(),
            schemaMap(),
            splits(),
            datasetName(),
            declaredReadSpec()
        );
    }
}
