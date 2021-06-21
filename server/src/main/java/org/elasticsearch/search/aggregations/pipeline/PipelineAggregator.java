/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.pipeline;


import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

public abstract class PipelineAggregator {
    /**
     * Parse the {@link PipelineAggregationBuilder} from a {@link XContentParser}.
     */
    @FunctionalInterface
    public interface Parser {
        ParseField BUCKETS_PATH = new ParseField("buckets_path");
        ParseField FORMAT = new ParseField("format");
        ParseField GAP_POLICY = new ParseField("gap_policy");

        /**
         * Returns the pipeline aggregator factory with which this parser is
         * associated.
         *
         * @param pipelineAggregatorName
         *            The name of the pipeline aggregation
         * @param parser the parser
         * @return The resolved pipeline aggregator factory
         * @throws java.io.IOException
         *             When parsing fails
         */
        PipelineAggregationBuilder parse(String pipelineAggregatorName, XContentParser parser)
                throws IOException;
    }

    /**
     * Tree of {@link PipelineAggregator}s to modify a tree of aggregations
     * after their final reduction.
     */
    public static class PipelineTree {
        /**
         * An empty tree of {@link PipelineAggregator}s.
         */
        public static final PipelineTree EMPTY = new PipelineTree(emptyMap(), emptyList());

        private final Map<String, PipelineTree> subTrees;
        private final List<PipelineAggregator> aggregators;

        public PipelineTree(Map<String, PipelineTree> subTrees, List<PipelineAggregator> aggregators) {
            this.subTrees = subTrees;
            this.aggregators = aggregators;
        }

        /**
         * The {@link PipelineAggregator}s for the aggregation at this
         * position in the tree.
         */
        public List<PipelineAggregator> aggregators() {
            return aggregators;
        }

        /**
         * Get the sub-tree at for the named sub-aggregation or {@link #EMPTY}
         * if there are no pipeline aggragations for that sub-aggregator.
         */
        public PipelineTree subTree(String name) {
            return subTrees.getOrDefault(name, EMPTY);
        }

        /**
         * Return {@code true} if this node in the tree has any subtrees.
         */
        public boolean hasSubTrees() {
            return false == subTrees.isEmpty();
        }

        @Override
        public String toString() {
            return "PipelineTree[" + aggregators + "," + subTrees + "]";
        }
    }

    private String name;
    private String[] bucketsPaths;
    private Map<String, Object> metadata;

    protected PipelineAggregator(String name, String[] bucketsPaths, Map<String, Object> metadata) {
        this.name = name;
        this.bucketsPaths = bucketsPaths;
        this.metadata = metadata;
    }

    public String name() {
        return name;
    }

    public String[] bucketsPaths() {
        return bucketsPaths;
    }

    public Map<String, Object> metadata() {
        return metadata;
    }

    public abstract InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext);
}
