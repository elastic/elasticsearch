/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.lucene.read.ComputeBlockLoaderFactory;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.search.fetch.FetchContext;
import org.elasticsearch.search.fetch.FetchSubPhase;
import org.elasticsearch.search.fetch.FetchSubPhaseProcessor;
import org.elasticsearch.search.fetch.StoredFieldsSpec;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xpack.esql.planner.EsPhysicalOperationProviders.DefaultShardContext;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Loads sequence fields with ordinary ES|QL's mapped block loaders on the PIT's selected Lucene document. This preserves
 * stored fields, keyword delegates, flattened reconstruction and array offsets without loading analyzed text fielddata.
 * Readers and compute blocks use the node's request breaker and are released with the fetch search context.
 */
public final class SequenceDocValuesFetchSubPhase implements FetchSubPhase {
    private final Supplier<BlockFactory> blockFactory;
    private final Supplier<ClusterSettings> clusterSettings;

    /** Defers access to the node's block factory until plugin components have been initialized. */
    public SequenceDocValuesFetchSubPhase(Supplier<BlockFactory> blockFactory, Supplier<ClusterSettings> clusterSettings) {
        this.blockFactory = blockFactory;
        this.clusterSettings = clusterSettings;
    }

    @Override
    public FetchSubPhaseProcessor getProcessor(FetchContext context) {
        var extension = (SequenceDocValuesSearchExt) context.getSearchExt(SequenceDocValuesSearchExt.NAME);
        if (extension == null || extension.fields().isEmpty()) {
            return null;
        }
        var searchContext = context.getSearchExecutionContext();
        var shardContext = new DefaultShardContext(0, () -> {}, searchContext, AliasFilter.EMPTY);
        var sizes = loaderSizes(clusterSettings.get());
        List<FieldLoader> fields = new ArrayList<>(extension.fields().size());
        for (var field : extension.fields()) {
            fields.add(
                new FieldLoader(
                    field.field,
                    shardContext.blockLoader(
                        field.field,
                        false,
                        extension.fieldExtractPreference(),
                        null,
                        (exceptionClass, message) -> HeaderWarning.addWarning("{}: {}", exceptionClass.getName(), message),
                        sizes.ordinals(),
                        sizes.script()
                    )
                )
            );
        }
        var processor = new Processor(context, fields, blockFactory.get());
        context.addReleasable(processor);
        return processor;
    }

    /** Captures live node/cluster reservation settings once for each fetch processor. */
    static LoaderSizes loaderSizes(ClusterSettings settings) {
        return new LoaderSizes(
            settings.get(PlannerSettings.BLOCK_LOADER_SIZE_ORDINALS),
            settings.get(PlannerSettings.BLOCK_LOADER_SIZE_SCRIPT)
        );
    }

    /** Reader reservations share the cluster defaults and dynamic updates used by ordinary ES|QL. */
    record LoaderSizes(ByteSizeValue ordinals, ByteSizeValue script) {}

    private static final class Processor implements FetchSubPhaseProcessor, Releasable {
        private final FetchContext context;
        private final List<FieldLoader> fields;
        private final BlockFactory blockFactory;
        private final ComputeBlockLoaderFactory loaderFactory;
        private final StoredFieldsSpec storedFields;

        private Processor(FetchContext context, List<FieldLoader> fields, BlockFactory blockFactory) {
            this.context = context;
            this.fields = fields;
            this.blockFactory = blockFactory;
            this.loaderFactory = new ComputeBlockLoaderFactory(blockFactory);
            this.storedFields = StoredFieldsSpec.build(fields, field -> field.loader.rowStrideStoredFieldSpec());
        }

        @Override
        public void setNextReader(LeafReaderContext readerContext) {
            for (FieldLoader field : fields) {
                field.setNextReader(readerContext);
            }
        }

        @Override
        public StoredFieldsSpec storedFieldsSpec() {
            return storedFields;
        }

        @Override
        public void process(HitContext hit) throws IOException {
            for (FieldLoader field : fields) {
                try (Block block = field.read(hit, blockFactory, loaderFactory)) {
                    Object value = BlockUtils.toJavaObject(block, 0);
                    List<Object> values;
                    if (value == null) {
                        values = List.of();
                    } else if (value instanceof List<?> list) {
                        values = new ArrayList<>(list);
                    } else {
                        values = List.of(value);
                    }
                    var documentField = new DocumentField(field.name, values);
                    // FetchPhase now accounts DocumentField estimates after all sub-phases. Only native backing arrays
                    // remain unaccounted; the existing retained-fetch checker releases their reservation with the response.
                    context.chargeInnerHitsBytes(additionalRetainedBytes(documentField));
                    hit.hit().setDocumentField(documentField);
                }
            }
        }

        @Override
        public void close() {
            Releasables.close(Releasables.wrap(fields), loaderFactory);
        }
    }

    /** Charges only copied native arrays beyond the field estimate already owned by FetchPhase. */
    static long additionalRetainedBytes(DocumentField field) {
        return SequenceHitReader.backingBytes(field);
    }

    /** A mutable reader cache kept only for the current leaf and fetch processor's lifetime. */
    static final class FieldLoader implements Releasable {
        private final String name;
        private final BlockLoader loader;
        private LeafReaderContext leaf;
        private BlockLoader.ColumnAtATimeReader columnReader;
        private BlockLoader.RowStrideReader rowReader;

        FieldLoader(String name, BlockLoader loader) {
            this.name = name;
            this.loader = loader;
        }

        void setNextReader(LeafReaderContext leaf) {
            close();
            this.leaf = leaf;
        }

        Block read(HitContext hit, BlockFactory blockFactory, ComputeBlockLoaderFactory loaderFactory) throws IOException {
            int doc = hit.docId();
            if (columnReader != null && columnReader.canReuse(doc) == false || rowReader != null && rowReader.canReuse(doc) == false) {
                close();
            }
            if (columnReader == null && rowReader == null) {
                var columnFactory = loader.columnAtATimeReader(leaf);
                if (columnFactory != null) {
                    columnReader = columnFactory.apply(blockFactory.breaker());
                } else {
                    rowReader = loader.rowStrideReader(blockFactory.breaker(), leaf);
                }
            }
            if (columnReader != null) {
                // ComputeBlockLoaderFactory guarantees that mapped loaders produce compute blocks.
                return (Block) columnReader.read(loaderFactory, new SingleDoc(doc), 0, false);
            }
            try (var builder = loader.builder(loaderFactory, 1)) {
                rowReader.read(doc, new HitStoredFields(hit), builder);
                return (Block) builder.build();
            }
        }

        @Override
        public void close() {
            var column = columnReader;
            var row = rowReader;
            columnReader = null;
            rowReader = null;
            Releasables.close(column, row);
        }
    }

    private record SingleDoc(int doc) implements BlockLoader.Docs {
        @Override
        public int count() {
            return 1;
        }

        @Override
        public int get(int index) {
            assert index == 0;
            return doc;
        }

        @Override
        public boolean mayContainDuplicates() {
            return false;
        }
    }

    private record HitStoredFields(HitContext hit) implements BlockLoader.StoredFields {
        @Override
        public Source source() {
            return hit.source();
        }

        @Override
        public String id() {
            return hit.hit().getId();
        }

        @Override
        public String routing() {
            var routing = hit.loadedFields().get("_routing");
            return routing == null || routing.isEmpty() ? null : routing.getFirst().toString();
        }

        @Override
        public Map<String, List<Object>> storedFields() {
            return hit.loadedFields();
        }

        @Override
        public boolean loaded() {
            return true;
        }
    }
}
