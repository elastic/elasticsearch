/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.planner;

import org.apache.lucene.index.IndexReader;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.sql.action.compute.lucene.LuceneSourceOperator;
import org.elasticsearch.xpack.sql.action.compute.lucene.NumericDocValuesExtractor;
import org.elasticsearch.xpack.sql.action.compute.operator.Driver;
import org.elasticsearch.xpack.sql.action.compute.operator.LongAvgOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.Operator;
import org.elasticsearch.xpack.sql.action.compute.operator.OutputOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.Exchange;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.ExchangeSinkOperator;
import org.elasticsearch.xpack.sql.action.compute.operator.exchange.ExchangeSourceOperator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * The local execution planner takes a plan (represented as PlanNode tree / digraph) as input and creates the corresponding
 * drivers that are used to execute the given plan.
 */
public class LocalExecutionPlanner {

    private final IndexReader indexReader;
    // TODO: allow configuring the following fields
    private final int defaultTaskConcurrency = ThreadPool.searchThreadPoolSize(EsExecutors.allocatedProcessors(Settings.EMPTY));
    private final int bufferMaxPages = 500;

    public LocalExecutionPlanner(IndexReader indexReader) {
        this.indexReader = indexReader;
    }

    /**
     * turn the given plan into a list of drivers to execute
     */
    public LocalExecutionPlan plan(PlanNode node) {
        LocalExecutionPlanContext context = new LocalExecutionPlanContext();

        PhysicalOperation physicalOperation = plan(node, context);

        context.addDriverFactory(
            new DriverFactory(() -> new Driver(physicalOperation.operators(), () -> {}), context.getDriverInstanceCount())
        );

        LocalExecutionPlan localExecutionPlan = new LocalExecutionPlan();
        localExecutionPlan.driverFactories.addAll(context.driverFactories);
        return localExecutionPlan;
    }

    public PhysicalOperation plan(PlanNode node, LocalExecutionPlanContext context) {
        if (node instanceof PlanNode.AggregationNode aggregationNode) {
            PhysicalOperation source = plan(aggregationNode.source, context);
            Map<String, Integer> layout = new HashMap<>();
            Supplier<Operator> operatorFactory = null;
            for (Map.Entry<String, PlanNode.AggregationNode.AggType> e : aggregationNode.aggs.entrySet()) {
                if (e.getValue()instanceof PlanNode.AggregationNode.AvgAggType avgAggType) {
                    if (aggregationNode.mode == PlanNode.AggregationNode.Mode.PARTIAL) {
                        operatorFactory = () -> new LongAvgOperator(source.layout.get(avgAggType.field()));
                        layout.put(e.getKey() + "_sum", 0);
                        layout.put(e.getKey() + "_count", 1);
                    } else {
                        operatorFactory = () -> new LongAvgOperator(
                            source.layout.get(e.getKey() + "_sum"),
                            source.layout.get(e.getKey() + "_count")
                        );
                        layout.put(e.getKey(), 0);
                    }
                }
            }
            if (operatorFactory != null) {
                return new PhysicalOperation(operatorFactory, layout, source);
            }
        } else if (node instanceof PlanNode.LuceneSourceNode luceneSourceNode) {
            Supplier<Operator> operatorFactory;
            if (luceneSourceNode.parallelism == PlanNode.LuceneSourceNode.Parallelism.SINGLE) {
                context.setDriverInstanceCount(1);
                operatorFactory = () -> new LuceneSourceOperator(indexReader, luceneSourceNode.query);
            } else if (luceneSourceNode.parallelism == PlanNode.LuceneSourceNode.Parallelism.SEGMENT) {
                context.setDriverInstanceCount(LuceneSourceOperator.numSegmentSlices(indexReader));
                AtomicReference<List<LuceneSourceOperator>> luceneSourceOperatorAtomicReference = new AtomicReference<>();
                AtomicInteger sliceCount = new AtomicInteger();
                operatorFactory = () -> {
                    if (luceneSourceOperatorAtomicReference.get() == null) {
                        luceneSourceOperatorAtomicReference.set(
                            new LuceneSourceOperator(indexReader, luceneSourceNode.query).segmentSlice()
                        );
                    }
                    return luceneSourceOperatorAtomicReference.get().get(sliceCount.getAndIncrement());
                };
            } else if (luceneSourceNode.parallelism == PlanNode.LuceneSourceNode.Parallelism.DOC) {
                context.setDriverInstanceCount(LuceneSourceOperator.numDocSlices(indexReader, defaultTaskConcurrency));
                AtomicReference<List<LuceneSourceOperator>> luceneSourceOperatorAtomicReference = new AtomicReference<>();
                AtomicInteger sliceCount = new AtomicInteger();
                operatorFactory = () -> {
                    if (luceneSourceOperatorAtomicReference.get() == null) {
                        luceneSourceOperatorAtomicReference.set(
                            new LuceneSourceOperator(indexReader, luceneSourceNode.query).docSlice(defaultTaskConcurrency)
                        );
                    }
                    return luceneSourceOperatorAtomicReference.get().get(sliceCount.getAndIncrement());
                };
            } else {
                throw new UnsupportedOperationException();
            }
            return new PhysicalOperation(operatorFactory, Map.of("_doc_id", 0, "_segment_id", 1));
        } else if (node instanceof PlanNode.NumericDocValuesSourceNode numericDocValuesSourceNode) {
            PhysicalOperation source = plan(numericDocValuesSourceNode.source, context);
            Map<String, Integer> layout = new HashMap<>();
            layout.putAll(source.layout);
            layout.put(numericDocValuesSourceNode.field, layout.size());
            return new PhysicalOperation(
                () -> new NumericDocValuesExtractor(
                    indexReader,
                    source.layout.get("_doc_id"),
                    source.layout.get("_segment_id"),
                    numericDocValuesSourceNode.field
                ),
                layout,
                source
            );
        } else if (node instanceof PlanNode.OutputNode outputNode) {
            PhysicalOperation source = plan(outputNode.source, context);
            String[] outputColumns = new String[source.layout.size()];
            for (Map.Entry<String, Integer> entry : source.layout.entrySet()) {
                outputColumns[entry.getValue()] = entry.getKey();
            }
            return new PhysicalOperation(
                () -> new OutputOperator(Arrays.asList(outputColumns), outputNode.pageConsumer),
                source.layout,
                source
            );
        } else if (node instanceof PlanNode.ExchangeNode exchangeNode) {
            int driverInstances;
            if (exchangeNode.type == PlanNode.ExchangeNode.Type.GATHER) {
                driverInstances = 1;
                context.setDriverInstanceCount(1);
            } else {
                driverInstances = defaultTaskConcurrency;
                context.setDriverInstanceCount(driverInstances);
            }
            Exchange exchange = new Exchange(driverInstances, exchangeNode.partitioning, bufferMaxPages);

            Map<String, Integer> layout = null;
            for (PlanNode sourceNode : exchangeNode.sources) {
                LocalExecutionPlanContext subContext = context.createSubContext();
                PhysicalOperation source = plan(sourceNode, subContext);
                layout = source.layout;
                PhysicalOperation physicalOperation = new PhysicalOperation(
                    () -> new ExchangeSinkOperator(exchange.createSink()),
                    source.layout,
                    source
                );
                context.addDriverFactory(
                    new DriverFactory(() -> new Driver(physicalOperation.operators(), () -> {}), subContext.getDriverInstanceCount())
                );
            }
            return new PhysicalOperation(() -> new ExchangeSourceOperator(exchange.getNextSource()), layout);
        }
        throw new UnsupportedOperationException();
    }

    public static class PhysicalOperation {
        private final List<Supplier<Operator>> operatorFactories = new ArrayList<>();
        private final Map<String, Integer> layout; // maps field names to channels

        PhysicalOperation(Supplier<Operator> operatorFactory, Map<String, Integer> layout) {
            this.operatorFactories.add(operatorFactory);
            this.layout = layout;
        }

        PhysicalOperation(Supplier<Operator> operatorFactory, Map<String, Integer> layout, PhysicalOperation source) {
            this.operatorFactories.addAll(source.operatorFactories);
            this.operatorFactories.add(operatorFactory);
            this.layout = layout;
        }

        public List<Operator> operators() {
            return operatorFactories.stream().map(Supplier::get).collect(Collectors.toList());
        }
    }

    /**
     * Context object used while generating a local plan. Currently only collects the driver factories as well as
     * maintains information how many driver instances should be created for a given driver.
     */
    public static class LocalExecutionPlanContext {
        final List<DriverFactory> driverFactories;
        int driverInstanceCount = 1;

        LocalExecutionPlanContext() {
            driverFactories = new ArrayList<>();
        }

        LocalExecutionPlanContext(List<DriverFactory> driverFactories) {
            this.driverFactories = driverFactories;
        }

        void addDriverFactory(DriverFactory driverFactory) {
            driverFactories.add(driverFactory);
        }

        public LocalExecutionPlanContext createSubContext() {
            LocalExecutionPlanContext subContext = new LocalExecutionPlanContext(driverFactories);
            return subContext;
        }

        public int getDriverInstanceCount() {
            return driverInstanceCount;
        }

        public void setDriverInstanceCount(int driverInstanceCount) {
            this.driverInstanceCount = driverInstanceCount;
        }
    }

    public record DriverFactory(Supplier<Driver> driverSupplier, int driverInstances) {

    }

    /**
     * Plan representation that is geared towards execution on a single node
     */
    public static class LocalExecutionPlan {
        final List<DriverFactory> driverFactories = new ArrayList<>();

        public List<Driver> createDrivers() {
            return driverFactories.stream()
                .flatMap(df -> IntStream.range(0, df.driverInstances).mapToObj(i -> df.driverSupplier.get()))
                .collect(Collectors.toList());
        }

        public List<DriverFactory> getDriverFactories() {
            return driverFactories;
        }
    }
}
