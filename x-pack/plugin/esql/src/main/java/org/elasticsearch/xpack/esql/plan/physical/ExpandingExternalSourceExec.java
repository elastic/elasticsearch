/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.datasources.DeclaredReadSpec;
import org.elasticsearch.xpack.esql.datasources.ExternalSchema;
import org.elasticsearch.xpack.esql.datasources.SchemaReconciliation;
import org.elasticsearch.xpack.esql.datasources.spi.ExpandRead;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSplit;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A dataset's {@code NESTED_EXPAND}, done inside the scan: the reader emits, after the scan's own columns, one row per
 * element of a collection of each file row, with the element's leaves as the {@link #expanded()} columns.
 * {@code PushNestedExpandToExternalSource} makes it from an {@link ExternalSourceExec} and removes the
 * {@code NestedExpandExec} it replaces.
 * <p>
 * A sibling of {@link ExternalSourceExec}, not a subclass: its rows are elements, so a rule written for file rows (a
 * pushed limit, the footer's statistics, a row position) must not match it unless it says so by matching
 * {@link AbstractExternalSourceExec}. It is local to the node that planned it: it never goes on the wire, where the
 * expansion would be lost and the parents returned unexpanded.
 */
public final class ExpandingExternalSourceExec extends AbstractExternalSourceExec<ExpandingExternalSourceExec> {

    /**
     * The read the expansion asks of the format reader.
     *
     * @param path        the collection, by its name in the files
     * @param leaves      per {@link #expanded()} column, the leaf it reads, by name relative to {@code path}
     * @param parentLimit the most file rows (parents) to expand, or {@link FormatReader#NO_LIMIT}. It is the copy of a
     *                    {@code LIMIT} above the expansion that sat right on the scan, so it bounds the parents a read
     *                    needs, never the rows the query returns
     */
    public record Expansion(String path, List<String> leaves, int parentLimit) {
        public Expansion {
            leaves = List.copyOf(leaves);
        }
    }

    // A property of info() of its own, so the attribute rewrites that walk info() reach the columns the scan adds
    private final List<Attribute> expanded;
    private final Expansion expansion;

    /** The ctor {@link #info()} uses: {@link ExternalSourceExec}'s plus the expanded columns and the expansion. */
    public ExpandingExternalSourceExec(
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
        DeclaredReadSpec declaredReadSpec,
        List<Attribute> expanded,
        Expansion expansion
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
            declaredReadSpec,
            expanded,
            expansion
        );
    }

    ExpandingExternalSourceExec(
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
        DeclaredReadSpec declaredReadSpec,
        List<Attribute> expanded,
        Expansion expansion
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
        this.expanded = List.copyOf(expanded);
        this.expansion = Objects.requireNonNull(expansion);
    }

    @Override
    protected ExpandingExternalSourceExec copy(
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
        return new ExpandingExternalSourceExec(
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
            declaredReadSpec,
            expanded,
            expansion
        );
    }

    /** The expanded columns the scan adds to its output, after its own, in output order. */
    public List<Attribute> expanded() {
        return expanded;
    }

    public Expansion expansion() {
        return expansion;
    }

    /** The scan's own columns, followed by the {@link #expanded()} ones. */
    @Override
    public List<Attribute> output() {
        List<Attribute> output = new ArrayList<>(attributes().size() + expanded.size());
        output.addAll(attributes());
        output.addAll(expanded);
        return output;
    }

    /** The read that asks the reader for the expansion's leaves, at the types of their columns. */
    public ExpandRead expandRead() {
        List<String> leaves = expansion.leaves();
        if (expanded.size() != leaves.size()) {
            throw new IllegalStateException("[" + expanded.size() + "] expanded columns for [" + leaves.size() + "] leaves");
        }
        List<ExpandRead.Leaf> read = new ArrayList<>(leaves.size());
        for (int i = 0; i < leaves.size(); i++) {
            read.add(new ExpandRead.Leaf(leaves.get(i), expanded.get(i).dataType()));
        }
        return new ExpandRead(expansion.path(), read);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw notSerialized();
    }

    @Override
    public String getWriteableName() {
        throw notSerialized();
    }

    private IllegalStateException notSerialized() {
        // The other local hints are re-derived where the scan runs, but the expansion replaced the NestedExpandExec above
        // the scan: dropped on the wire, it would return the parents unexpanded
        return new IllegalStateException("a scan that expands [" + expansion.path() + "] runs where it was planned");
    }

    @Override
    protected NodeInfo<ExpandingExternalSourceExec> info() {
        // The same properties as ExternalSourceExec#info, for the same reasons, then the expanded columns and the expansion
        return NodeInfo.create(
            this,
            ExpandingExternalSourceExec::new,
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
            declaredReadSpec(),
            expanded,
            expansion
        );
    }

    @Override
    protected void appendNodeDetails(StringBuilder sb) {
        sb.append("[expand=").append(expansion.path());
        if (expansion.parentLimit() != FormatReader.NO_LIMIT) {
            sb.append(",parentLimit=").append(expansion.parentLimit());
        }
        sb.append("]");
    }

    @Override
    public int hashCode() {
        return 31 * super.hashCode() + Objects.hash(expanded, expansion);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj) == false) {
            return false;
        }
        ExpandingExternalSourceExec other = (ExpandingExternalSourceExec) obj;
        return expanded.equals(other.expanded) && expansion.equals(other.expansion);
    }
}
