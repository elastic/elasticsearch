/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.planner;

import org.apache.lucene.search.Query;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * A plan is represented as a tree / digraph of nodes. There are different node types, each representing a different type of computation
 */
public abstract class PlanNode implements ToXContentObject {

    public static class LuceneSourceNode extends PlanNode {
        final Query query;
        final Parallelism parallelism;

        public LuceneSourceNode(Query query, Parallelism parallelism) {
            this.query = query;
            this.parallelism = parallelism;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("kind", "lucene-source");
            builder.field("query", query.toString());
            builder.field("parallelism", parallelism);
            builder.endObject();
            return builder;
        }

        public enum Parallelism {
            SINGLE,
            SEGMENT,
            DOC,
        }
    }

    public static class NumericDocValuesSourceNode extends PlanNode {
        final PlanNode source;
        final String field;

        public NumericDocValuesSourceNode(PlanNode source, String field) {
            this.source = source;
            this.field = field;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("kind", "doc-values");
            builder.field("field", field);
            builder.field("source", source);
            builder.endObject();
            return builder;
        }
    }

    public static class AggregationNode extends PlanNode {
        final PlanNode source;
        final Map<String, AggType> aggs; // map from agg_field_name to the aggregate (e.g. f_avg -> AVG(f))
        final Mode mode;

        public AggregationNode(PlanNode source, Map<String, AggType> aggs, Mode mode) {
            this.source = source;
            this.aggs = aggs;
            this.mode = mode;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("kind", "aggregation");
            builder.field("mode", mode);
            builder.startArray("aggs");
            for (Map.Entry<String, AggType> agg : aggs.entrySet()) {
                builder.startObject();
                builder.field("name", agg.getKey());
                agg.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endArray();
            builder.field("source", source);
            builder.endObject();
            return builder;
        }

        public interface AggType extends ToXContent {

        }

        public record AvgAggType(String field) implements AggType {

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field("operation", "AVG");
                builder.field("field", field);
                return builder;
            }
        }

        public enum Mode {
            PARTIAL, // maps raw inputs to intermediate outputs
            FINAL, // maps intermediate inputs to final outputs
        }
    }

    public static class ExchangeNode extends PlanNode {
        final Type type;
        final List<PlanNode> sources;
        final Partitioning partitioning;

        public ExchangeNode(Type type, List<PlanNode> sources, Partitioning partitioning) {
            this.type = type;
            this.sources = sources;
            this.partitioning = partitioning;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("kind", "exchange");
            builder.field("type", type);
            builder.field("partitioning", partitioning);
            if (sources.size() == 1) {
                builder.field("source", sources.get(0));
            } else {
                builder.startArray("sources");
                for (PlanNode source : sources) {
                    builder.value(source);
                }
                builder.endArray();
            }
            builder.endObject();
            return builder;
        }

        public enum Type {
            GATHER, // gathering results from various sources (1:n)
            REPARTITION, // repartitioning results from various sources (n:m)
            // REPLICATE, TODO: implement
        }

        public enum Partitioning {
            SINGLE_DISTRIBUTION, // single exchange source, no partitioning
            FIXED_ARBITRARY_DISTRIBUTION, // multiple exchange sources, random partitioning
            FIXED_BROADCAST_DISTRIBUTION, // multiple exchange sources, broadcasting
            FIXED_PASSTHROUGH_DISTRIBUTION, // n:n forwarding
            // FIXED_HASH_DISTRIBUTION, TODO: implement hash partitioning
        }
    }

    public static class OutputNode extends PlanNode {
        final PlanNode source;
        final BiConsumer<List<String>, Page> pageConsumer;

        public OutputNode(PlanNode source, BiConsumer<List<String>, Page> pageConsumer) {
            this.source = source;
            this.pageConsumer = pageConsumer;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field("kind", "output");
            builder.field("source", source);
            builder.endObject();
            return builder;
        }
    }

    /**
     * returns a fluent builder which allows creating a simple chain of plan nodes (bottom-up).
     */
    public static Builder builder(Query query, LuceneSourceNode.Parallelism parallelism) {
        return new Builder(new LuceneSourceNode(query, parallelism));
    }

    public static class Builder {
        private PlanNode current;

        public Builder(PlanNode current) {
            this.current = current;
        }

        /**
         * extract the numeric doc values for the given field
         */
        public Builder numericDocValues(String field) {
            current = new NumericDocValuesSourceNode(current, field);
            return this;
        }

        /**
         * compute the avg of the given field
         */
        public Builder avg(String field) {
            return avgPartial(field).avgFinal(field);
        }

        /**
         * partial computation of avg
         */
        public Builder avgPartial(String field) {
            current = new AggregationNode(
                current,
                Map.of(field + "_avg", new AggregationNode.AvgAggType(field)),
                AggregationNode.Mode.PARTIAL
            );
            return this;
        }

        /**
         * final computation of avg
         */
        public Builder avgFinal(String field) {
            current = new AggregationNode(
                current,
                Map.of(field + "_avg", new AggregationNode.AvgAggType(field)),
                AggregationNode.Mode.FINAL
            );
            return this;
        }

        /**
         * creates a local exchange of the given type and partitioning
         */
        public Builder exchange(ExchangeNode.Type type, ExchangeNode.Partitioning partitioning) {
            current = new ExchangeNode(type, Arrays.asList(current), partitioning);
            return this;
        }

        /**
         * builds and returns the given plan. Adds an output node at the top to ensure that the pages flowing through the plan
         * are actually consumed.
         */
        public PlanNode build(BiConsumer<List<String>, Page> pageConsumer) {
            return new OutputNode(current, pageConsumer);
        }

    }
}
