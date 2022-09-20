/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.action.compute.planner;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObjectHelper;
import org.elasticsearch.xpack.sql.action.compute.data.Page;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * A plan is represented as a tree / digraph of nodes. There are different node types, each representing a different type of computation
 */
public abstract class PlanNode implements NamedXContentObject {

    public static final ParseField SOURCE_FIELD = new ParseField("source");

    public static List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return List.of(
            new NamedXContentRegistry.Entry(
                PlanNode.class,
                LuceneSourceNode.LUCENE_SOURCE_FIELD,
                (p, c) -> LuceneSourceNode.PARSER.parse(p, null)
            ),
            new NamedXContentRegistry.Entry(
                PlanNode.class,
                NumericDocValuesSourceNode.DOC_VALUES_FIELD,
                (p, c) -> NumericDocValuesSourceNode.PARSER.parse(p, null)
            ),
            new NamedXContentRegistry.Entry(
                PlanNode.class,
                AggregationNode.AGGREGATION_FIELD,
                (p, c) -> AggregationNode.PARSER.parse(p, null)
            ),
            new NamedXContentRegistry.Entry(PlanNode.class, ExchangeNode.EXCHANGE_FIELD, (p, c) -> ExchangeNode.PARSER.parse(p, null)),
            new NamedXContentRegistry.Entry(
                AggregationNode.AggType.class,
                AggregationNode.AvgAggType.AVG_FIELD,
                (p, c) -> AggregationNode.AvgAggType.PARSER.parse(p, (String) c)
            )
        );
    }

    public abstract List<PlanNode> getSourceNodes();

    public String[] getIndices() {
        final Set<String> indices = new LinkedHashSet<>();
        getPlanNodesMatching(planNode -> planNode instanceof LuceneSourceNode).forEach(
            planNode -> indices.addAll(Arrays.asList(((LuceneSourceNode) planNode).indices))
        );
        return indices.toArray(String[]::new);
    }

    public static class LuceneSourceNode extends PlanNode {
        final Query query;
        final Parallelism parallelism;
        final String[] indices;

        public LuceneSourceNode(Query query, Parallelism parallelism, String... indices) {
            this.query = query;
            this.parallelism = parallelism;
            this.indices = indices;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(INDICES_FIELD.getPreferredName(), Arrays.toString(indices));
            builder.field(QUERY_FIELD.getPreferredName(), query.toString());
            builder.field(PARALLELISM_FIELD.getPreferredName(), parallelism);
            builder.endObject();
            return builder;
        }

        @Override
        public List<PlanNode> getSourceNodes() {
            return List.of();
        }

        @Override
        public String getName() {
            return LUCENE_SOURCE_FIELD.getPreferredName();
        }

        public enum Parallelism {
            SINGLE,
            SEGMENT,
            DOC,
        }

        public static final ParseField LUCENE_SOURCE_FIELD = new ParseField("lucene-source");
        public static final ParseField QUERY_FIELD = new ParseField("query");
        public static final ParseField PARALLELISM_FIELD = new ParseField("parallelism");
        public static final ParseField INDICES_FIELD = new ParseField("indices");

        static final ConstructingObjectParser<LuceneSourceNode, Void> PARSER = new ConstructingObjectParser<>(
            "lucene_source_node",
            args -> new LuceneSourceNode(
                "*:*".equals(args[0]) ? new MatchAllDocsQuery() : null,
                (Parallelism) args[1],
                ((List<?>) args[2]).toArray(String[]::new)
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), QUERY_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), Parallelism::valueOf, PARALLELISM_FIELD);
            PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), INDICES_FIELD);
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
            builder.field(FIELD_FIELD.getPreferredName(), field);
            NamedXContentObjectHelper.writeNamedObject(builder, params, SOURCE_FIELD.getPreferredName(), source);
            builder.endObject();
            return builder;
        }

        @Override
        public List<PlanNode> getSourceNodes() {
            return Arrays.asList(source);
        }

        public static final ParseField FIELD_FIELD = new ParseField("field");

        static final ConstructingObjectParser<NumericDocValuesSourceNode, Void> PARSER = new ConstructingObjectParser<>(
            "doc_values_node",
            args -> new NumericDocValuesSourceNode((PlanNode) args[0], (String) args[1])
        );

        static {
            PARSER.declareNamedObject(
                ConstructingObjectParser.constructorArg(),
                (p, c, n) -> p.namedObject(PlanNode.class, n, c),
                SOURCE_FIELD
            );
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD_FIELD);
        }

        public static final ParseField DOC_VALUES_FIELD = new ParseField("doc-values");

        @Override
        public String getName() {
            return DOC_VALUES_FIELD.getPreferredName();
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

        public static final ParseField MODE_FIELD = new ParseField("mode");
        public static final ParseField AGGS_FIELD = new ParseField("aggs");

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(MODE_FIELD.getPreferredName(), mode);
            builder.startObject(AGGS_FIELD.getPreferredName());
            for (Map.Entry<String, AggType> agg : aggs.entrySet()) {
                NamedXContentObjectHelper.writeNamedObject(builder, params, agg.getKey(), agg.getValue());
            }
            builder.endObject();
            NamedXContentObjectHelper.writeNamedObject(builder, params, SOURCE_FIELD.getPreferredName(), source);
            builder.endObject();
            return builder;
        }

        @SuppressWarnings("unchecked")
        static final ConstructingObjectParser<AggregationNode, Void> PARSER = new ConstructingObjectParser<>(
            "aggregation_node",
            args -> new AggregationNode(
                (PlanNode) args[0],
                ((List<Tuple<String, AggType>>) args[1]).stream().collect(Collectors.toMap(Tuple::v1, Tuple::v2)),
                (Mode) args[2]
            )
        );

        static {
            PARSER.declareNamedObject(
                ConstructingObjectParser.constructorArg(),
                (p, c, n) -> p.namedObject(PlanNode.class, n, c),
                SOURCE_FIELD
            );
            PARSER.declareNamedObjects(ConstructingObjectParser.constructorArg(), (p, c, n) -> {
                XContentParser.Token token = p.nextToken();
                assert token == XContentParser.Token.START_OBJECT;
                token = p.nextToken();
                assert token == XContentParser.Token.FIELD_NAME;
                String commandName = p.currentName();
                AggType agg = p.namedObject(AggType.class, commandName, c);
                token = p.nextToken();
                assert token == XContentParser.Token.END_OBJECT;
                return Tuple.tuple(n, agg);
            }, AGGS_FIELD);
            PARSER.declareString(ConstructingObjectParser.constructorArg(), Mode::valueOf, MODE_FIELD);
        }

        public static final ParseField AGGREGATION_FIELD = new ParseField("aggregation");

        @Override
        public List<PlanNode> getSourceNodes() {
            return Arrays.asList(source);
        }

        @Override
        public String getName() {
            return AGGREGATION_FIELD.getPreferredName();
        }

        public interface AggType extends NamedXContentObject {

        }

        public record AvgAggType(String field, Type type) implements AggType {

            static final ConstructingObjectParser<AggType, String> PARSER = new ConstructingObjectParser<>(
                "avg_agg_type",
                args -> new AvgAggType((String) args[0], args[1] == null ? Type.DOUBLE : (Type) args[1])
            );

            public static final ParseField FIELD_FIELD = new ParseField("field");
            public static final ParseField TYPE_FIELD = new ParseField("type");

            static {
                PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD_FIELD);
                PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), Type::valueOf, TYPE_FIELD);
            }

            @Override
            public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
                builder.startObject();
                builder.field(FIELD_FIELD.getPreferredName(), field);
                builder.field(TYPE_FIELD.getPreferredName(), type);
                builder.endObject();
                return builder;
            }

            public static final ParseField AVG_FIELD = new ParseField("avg");

            @Override
            public String getName() {
                return AVG_FIELD.getPreferredName();
            }

            public enum Type {
                LONG,
                DOUBLE
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

        public static final ParseField TYPE_FIELD = new ParseField("type");
        public static final ParseField PARTITIONING_FIELD = new ParseField("partitioning");

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(TYPE_FIELD.getPreferredName(), type);
            builder.field(PARTITIONING_FIELD.getPreferredName(), partitioning);
            if (sources.size() == 1) {
                NamedXContentObjectHelper.writeNamedObject(builder, params, SOURCE_FIELD.getPreferredName(), sources.get(0));
            } else {
                throw new UnsupportedOperationException();
            }
            builder.endObject();
            return builder;
        }

        static final ConstructingObjectParser<ExchangeNode, Void> PARSER = new ConstructingObjectParser<>(
            "exchange_node",
            args -> new ExchangeNode((Type) args[0], List.of((PlanNode) args[1]), (Partitioning) args[2])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), Type::valueOf, TYPE_FIELD);
            PARSER.declareNamedObject(
                ConstructingObjectParser.constructorArg(),
                (p, c, n) -> p.namedObject(PlanNode.class, n, c),
                SOURCE_FIELD
            );
            PARSER.declareString(ConstructingObjectParser.constructorArg(), Partitioning::valueOf, PARTITIONING_FIELD);
        }

        @Override
        public List<PlanNode> getSourceNodes() {
            return sources;
        }

        public static final ParseField EXCHANGE_FIELD = new ParseField("exchange");

        @Override
        public String getName() {
            return EXCHANGE_FIELD.getPreferredName();
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
            NamedXContentObjectHelper.writeNamedObject(builder, params, SOURCE_FIELD.getPreferredName(), source);
            builder.endObject();
            return builder;
        }

        public static final ParseField OUTPUT_FIELD = new ParseField("output");

        @Override
        public List<PlanNode> getSourceNodes() {
            return Arrays.asList(source);
        }

        @Override
        public String getName() {
            return OUTPUT_FIELD.getPreferredName();
        }
    }

    /**
     * returns a fluent builder which allows creating a simple chain of plan nodes (bottom-up).
     */
    public static Builder builder(Query query, LuceneSourceNode.Parallelism parallelism, String... indices) {
        return new Builder(new LuceneSourceNode(query, parallelism, indices));
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
                Map.of(field + "_avg", new AggregationNode.AvgAggType(field, AggregationNode.AvgAggType.Type.DOUBLE)),
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
                Map.of(field + "_avg", new AggregationNode.AvgAggType(field, AggregationNode.AvgAggType.Type.DOUBLE)),
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

        public PlanNode buildWithoutOutputNode() {
            return current;
        }

    }

    public List<PlanNode> getPlanNodesMatching(Predicate<PlanNode> planNodePredicate) {
        List<PlanNode> matchingNodes = new ArrayList<>();
        if (planNodePredicate.test(this)) {
            matchingNodes.add(this);
        }
        for (PlanNode planNode : getSourceNodes()) {
            matchingNodes.addAll(planNode.getPlanNodesMatching(planNodePredicate));
        }
        return matchingNodes;
    }
}
