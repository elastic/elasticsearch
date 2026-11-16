/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.function.scalar.nested;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.DocVector;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.AbstractPageMappingOperator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.operator.Operator;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Appends the value of each reduction of an EVAL to each parent. The reductions over one nested field read its children
 * once, together, rather than once each.
 */
public final class NestedReduceOperator extends AbstractPageMappingOperator {
    /**
     * Reductions over one nested field and the read of its children they share. Block 0 of each page {@code children} gives
     * is the parent positions; {@code channels} names, for each reduction, block 0 and then the blocks of its child fields.
     */
    public record Group(NestedChildren.Factory children, List<NestedReduction.ReducerFactory> reducers, List<int[]> channels) {
        public Reader get(DriverContext context) {
            List<NestedReduction.Reducer> built = new ArrayList<>(reducers.size());
            NestedChildren read = null;
            try {
                for (var reducer : reducers) {
                    built.add(reducer.get(context));
                }
                read = children.get(context);
                return new Reader(read, built, channels);
            } catch (RuntimeException | Error e) {
                Releasables.closeWhileHandlingException(Releasables.wrap(built), read);
                throw e;
            }
        }

        @Override
        public String toString() {
            return reducers.stream().map(Object::toString).collect(Collectors.joining(", ", "[", "]"));
        }
    }

    /** One driver's {@link Group}. */
    public static final class Reader implements Releasable {
        private final NestedChildren children;
        private final List<NestedReduction.Reducer> reducers;
        private final List<int[]> channels;

        private Reader(NestedChildren children, List<NestedReduction.Reducer> reducers, List<int[]> channels) {
            this.children = children;
            this.reducers = reducers;
            this.channels = channels;
        }

        /** One block per reduction, with its value for each parent of {@code parents}. */
        public Block[] reduce(Page parents) {
            DocVector docs = NestedChildren.docs(parents);
            if (docs == null) {
                throw new IllegalStateException("NESTED needs the parent documents, which this page does not carry");
            }
            NestedReduction.Reducer.Pass[] passes = new NestedReduction.Reducer.Pass[reducers.size()];
            Block[] results = new Block[reducers.size()];
            try {
                for (int r = 0; r < passes.length; r++) {
                    passes[r] = reducers.get(r).start(docs.getPositionCount());
                }
                children.read(docs, loaded -> {
                    for (int r = 0; r < passes.length; r++) {
                        Page fields = loaded.projectBlocks(channels.get(r));
                        try {
                            passes[r].accept(fields);
                        } finally {
                            fields.releaseBlocks();
                        }
                    }
                });
                for (int r = 0; r < passes.length; r++) {
                    results[r] = passes[r].finish();
                }
                Block[] reduced = results;
                results = null;
                return reduced;
            } finally {
                Releasables.closeExpectNoException(passes);
                if (results != null) {
                    Releasables.closeExpectNoException(results);
                }
            }
        }

        long baseRamBytesUsed() {
            return reducers.stream().mapToLong(NestedReduction.Reducer::baseRamBytesUsed).sum();
        }

        @Override
        public void close() {
            Releasables.close(Releasables.wrap(reducers), children);
        }
    }

    public record Factory(List<Group> groups) implements Operator.OperatorFactory {
        @Override
        public Operator get(DriverContext context) {
            List<Reader> readers = new ArrayList<>(groups.size());
            try {
                for (Group group : groups) {
                    readers.add(group.get(context));
                }
                return new NestedReduceOperator(readers);
            } catch (RuntimeException | Error e) {
                Releasables.closeWhileHandlingException(Releasables.wrap(readers));
                throw e;
            }
        }

        @Override
        public String describe() {
            return "NestedReduceOperator" + groups;
        }
    }

    private final List<Reader> readers;

    private NestedReduceOperator(List<Reader> readers) {
        this.readers = readers;
    }

    @Override
    protected Page process(Page page) {
        List<Block> blocks = new ArrayList<>();
        try {
            for (Reader reader : readers) {
                blocks.addAll(List.of(reader.reduce(page)));
            }
            Page result = page.appendBlocks(blocks.toArray(Block[]::new));
            blocks.clear();
            page = null;
            return result;
        } finally {
            Releasables.closeExpectNoException(blocks);
            if (page != null) {
                page.releaseBlocks();
            }
        }
    }

    @Override
    public String toString() {
        return "NestedReduceOperator[" + readers.size() + " nested fields]";
    }

    @Override
    public void close() {
        Releasables.closeExpectNoException(Releasables.wrap(readers), super::close);
    }
}
