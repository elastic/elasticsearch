/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * An Aggregator.
 * <p>
 * Be <strong>careful</strong> when adding methods to this class. If possible
 * make sure they have sensible default implementations.
 */
public abstract class Aggregator extends BucketCollector implements Releasable {

    /**
     * Parses the aggregation request and creates the appropriate aggregator factory for it.
     *
     * @see AggregationBuilder
     */
    @FunctionalInterface
    public interface Parser {
        /**
         * Returns the aggregator factory with which this parser is associated, may return {@code null} indicating the
         * aggregation should be skipped (e.g. when trying to aggregate on unmapped fields).
         *
         * @param aggregationName   The name of the aggregation
         * @param parser            The parser
         * @return                  The resolved aggregator factory or {@code null} in case the aggregation should be skipped
         * @throws java.io.IOException      When parsing fails
         */
        AggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException;
    }

    /**
     * Return the name of this aggregator.
     */
    public abstract String name();

    /**
     * Return the parent aggregator.
     */
    public abstract Aggregator parent();

    /**
     * Return the sub aggregator with the provided name.
     */
    public abstract Aggregator subAggregator(String name);

    /**
     * Resolve the next step of the sort path as though this aggregation
     * supported sorting. This is usually the "first step" when resolving
     * a sort path because most aggs that support sorting their buckets
     * aren't valid in the middle of a sort path.
     * <p>
     * For example, the {@code terms} aggs supports sorting its buckets, but
     * that sort path itself can't contain a different {@code terms}
     * aggregation.
     */
    public final Aggregator resolveSortPathOnValidAgg(AggregationPath.PathElement next, Iterator<AggregationPath.PathElement> path) {
        Aggregator n = subAggregator(next.name());
        if (n == null) {
            throw new IllegalArgumentException(
                "The provided aggregation ["
                    + next
                    + "] either does not exist, or is "
                    + "a pipeline aggregation and cannot be used to sort the buckets."
            );
        }
        if (false == path.hasNext()) {
            return n;
        }
        if (next.key() != null) {
            throw new IllegalArgumentException("Key only allowed on last aggregation path element but got [" + next + "]");
        }
        return n.resolveSortPath(path.next(), path);
    }

    /**
     * Resolve a sort path to the target.
     * <p>
     * The default implementation throws an exception but we override it on aggregations that support sorting.
     */
    public Aggregator resolveSortPath(AggregationPath.PathElement next, Iterator<AggregationPath.PathElement> path) {
        throw new IllegalArgumentException(
            "Buckets can only be sorted on a sub-aggregator path "
                + "that is built out of zero or more single-bucket aggregations within the path and a final "
                + "single-bucket or a metrics aggregation at the path end. ["
                + name()
                + "] is not single-bucket."
        );
    }

    /**
     * Builds a comparator that compares two buckets aggregated by this {@linkplain Aggregator}.
     * <p>
     * The default implementation throws an exception but we override it on aggregations that support sorting.
     */
    public BucketComparator bucketComparator(String key, SortOrder order) {
        throw new IllegalArgumentException(
            "Buckets can only be sorted on a sub-aggregator path "
                + "that is built out of zero or more single-bucket aggregations within the path and a final "
                + "single-bucket or a metrics aggregation at the path end."
        );
    }

    /**
     * Compare two buckets by their ordinal.
     */
    @FunctionalInterface
    public interface BucketComparator {
        /**
         * Compare two buckets by their ordinal.
         */
        int compare(long lhs, long rhs);
    }

    /**
     * Build the results of this aggregation.
      * @param ordsToCollect the ordinals of the buckets that we want to
     *        collect from this aggregation
     * @return the results for each ordinal, in the same order as the array
     *         of ordinals
     */
    public abstract InternalAggregation[] buildAggregations(long[] ordsToCollect) throws IOException;

    /**
     * Release this aggregation and its sub-aggregations.
     */
    public abstract void releaseAggregations();

    /**
     * Build the result of this aggregation if it is at the "top level"
     * of the aggregation tree. If, instead, it is a sub-aggregation of
     * another aggregation then the aggregation that contains it will call
     * {@link #buildAggregations(long[])}.
     */
    public final InternalAggregation buildTopLevel() throws IOException {
        assert parent() == null;
        return buildAggregations(new long[] { 0 })[0];
    }

    /**
     * Build an empty aggregation.
     */
    public abstract InternalAggregation buildEmptyAggregation();

    /**
     * Collect debug information to add to the profiling results. This will
     * only be called if the aggregation is being profiled.
     * <p>
     * Well behaved implementations will always call the superclass
     * implementation just in case it has something interesting. They will
     * also only add objects which can be serialized with
     * {@link StreamOutput#writeGenericValue(Object)} and
     * {@link XContentBuilder#value(Object)}. And they'll have an integration
     * test.
     */
    public void collectDebugInfo(BiConsumer<String, Object> add) {}

    /**
     * Get the aggregators running under this one.
     */
    public abstract Aggregator[] subAggregators();

    /**
     * Preform a dense merge (i.e. {@link org.elasticsearch.common.util.BigArray} to {@link org.elasticsearch.common.util.BigArray})
     * merge of the given aggregators. This may be done in place, or may involve allocating a new BigArray.
     * <p>
     * We use the term {@code merge leader} to refer to the instance driving the merge, typically the instance that
     * had its merge method called.  In the case of an in place merge, this aggregator will have its doc counts updated
     * (and possibly have new buckets added, if there were keys it didn't collect).  This matters less for a copy merge,
     * but is still relevant in that the merge leader is the aggregation which will persist after the merge.  All
     * non-leader aggregations can be closed after merging.
     * <p>
     * NB: It is expected that the merge leader is included in the list of aggregators to merge
     * <p>
     * The input map keys should be the owning bucket ordinals for the result aggregation.  For each such key,
     * there should be a list of aggregator references paired with the owning bucket ord corresponding to that
     * key in the given aggregation.  It is expected that an aggregator will be referenced from many such lists,
     * but within each such list the aggregators must be distinct.
     *
     * @param toMerge
     * @param bigArrays
     */
    public void merge(Map<Long, List<AggregationAndBucket>> toMerge, BigArrays bigArrays) {
        // NOCOMMIT: this should be abstract
        throw new UnsupportedOperationException();
    }

    public record AggregationAndBucket(
        long bucketOrdinal,
        Aggregator aggregator
    ) {

    }

    /** Aggregation mode for sub aggregations. */
    public enum SubAggCollectionMode implements Writeable {

        /**
         * Creates buckets and delegates to child aggregators in a single pass over
         * the matching documents
         */
        DEPTH_FIRST(new ParseField("depth_first")),

        /**
         * Creates buckets for all matching docs and then prunes to top-scoring buckets
         * before a second pass over the data when child aggregators are called
         * but only for docs from the top-scoring buckets
         */
        BREADTH_FIRST(new ParseField("breadth_first"));

        public static final ParseField KEY = new ParseField("collect_mode");

        private final ParseField parseField;

        SubAggCollectionMode(ParseField parseField) {
            this.parseField = parseField;
        }

        public ParseField parseField() {
            return parseField;
        }

        public static SubAggCollectionMode parse(String value, DeprecationHandler deprecationHandler) {
            SubAggCollectionMode[] modes = SubAggCollectionMode.values();
            for (SubAggCollectionMode mode : modes) {
                if (mode.parseField.match(value, deprecationHandler)) {
                    return mode;
                }
            }
            throw new ElasticsearchParseException("no [{}] found for value [{}]", KEY.getPreferredName(), value);
        }

        public static SubAggCollectionMode readFromStream(StreamInput in) throws IOException {
            return in.readEnum(SubAggCollectionMode.class);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeEnum(this);
        }
    }
}
