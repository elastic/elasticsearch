/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.search.aggregations.AggregationErrors;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.Aggregator.BucketComparator;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;
import org.elasticsearch.search.profile.aggregation.ProfilingAggregator;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A path that can be used to sort/order buckets (in some multi-bucket aggregations, e.g. terms &amp; histogram) based on
 * sub-aggregations. The path may point to either a single-bucket aggregation or a metrics aggregation. If the path
 * points to a single-bucket aggregation, the sort will be applied based on the {@code doc_count} of the bucket. If this
 * path points to a metrics aggregation, if it's a single-value metrics (eg. avg, max, min, etc..) the sort will be
 * applied on that single value. If it points to a multi-value metrics, the path should point out what metric should be
 * the sort-by value.
 * <p>
 * The path has the following form:
 * {@code <aggregation_name>['>'<aggregation_name>*]['.'<metric_name>]}
 * <p>
 * Examples:
 *
 * <ul>
 *     <li>
 *         {@code agg1>agg2>agg3} - where agg1, agg2 and agg3 are all single-bucket aggs (eg filter, nested, missing, etc..). In
 *                                  this case, the order will be based on the number of documents under {@code agg3}.
 *     </li>
 *     <li>
 *         {@code agg1>agg2>agg3} - where agg1 and agg2 are both single-bucket aggs and agg3 is a single-value metrics agg (eg avg, max,
 *                                  min, etc..). In this case, the order will be based on the value of {@code agg3}.
 *     </li>
 *     <li>
 *         {@code agg1>agg2>agg3.avg} - where agg1 and agg2 are both single-bucket aggs and agg3 is a multi-value metrics agg (eg stats,
 *                                  extended_stats, etc...). In this case, the order will be based on the avg value of {@code agg3}.
 *     </li>
 *     <li>
 *         {@code agg1["foo"]>agg2>agg3.avg} - where agg1 is multi-bucket, and the path expects a bucket "foo",
 *                                              agg2 are both single-bucket aggs and agg3 is a multi-value metrics agg
 *                                              (eg stats, extended_stats, etc...).
 *                                              In this case, the order will be based on the avg value of {@code agg3}.
 *     </li>
 *     <li>
 *         {@code agg1["foo"]._count} - where agg1 is multi-bucket, and the path expects a bucket "foo".
 *                                      This would extract the doc_count for that specific bucket.
 *     </li>
 * </ul>
 *
 */
public class AggregationPath {

    private static final String AGG_DELIM = ">";

    /**
     * Indicates if the current path element contains a bucket key.
     *
     * InternalMultiBucketAggregation#resolvePropertyFromPath supports resolving specific buckets and a bucket is indicated by
     * wrapping a key element in quotations. Example `agg['foo']` would get the bucket `foo` in the agg.
     *
     * @param pathElement The path element to check
     * @return Does the path element contain a bucket_key or not
     */
    public static boolean pathElementContainsBucketKey(AggregationPath.PathElement pathElement) {
        return pathElement != null && pathElement.key() != null && pathElement.key().startsWith("'") && pathElement.key().endsWith("'");
    }

    public static List<String> pathElementsAsStringList(List<PathElement> pathElements) {
        List<String> stringPathElements = new ArrayList<>();
        for (PathElement pathElement : pathElements) {
            stringPathElements.add(pathElement.name);
            if (pathElement.key != null) {
                stringPathElements.add(pathElement.key);
            }
            if (pathElement.metric != null) {
                stringPathElements.add(pathElement.metric);
            }
        }
        return stringPathElements;
    }

    public static AggregationPath parse(String path) {
        String[] elements = Strings.tokenizeToStringArray(path, AGG_DELIM);
        List<PathElement> tokens = new ArrayList<>(elements.length);
        String[] tuple = new String[2];
        for (int i = 0; i < elements.length; i++) {
            String element = elements[i];
            if (i == elements.length - 1) {
                int keyIndex = element.lastIndexOf('[');
                int metricIndex = element.lastIndexOf('.');
                if (keyIndex >= 0) {
                    if (keyIndex == 0 || keyIndex > element.length() - 3) {
                        throw AggregationErrors.invalidPathElement(element, path);
                    }
                    int endKeyIndex = element.lastIndexOf(']');
                    if (endKeyIndex < keyIndex) {
                        throw AggregationErrors.invalidPathElement(element, path);
                    }
                    if (metricIndex < 0 && endKeyIndex != element.length() - 1) {
                        throw AggregationErrors.invalidPathElement(element, path);
                    }
                    tokens.add(
                        new PathElement(
                            element,
                            element.substring(0, keyIndex),
                            element.substring(keyIndex + 1, endKeyIndex),
                            // Aggs and metrics can have `.` in the name, so only count as a metric in a bucket if after the brackets
                            metricIndex < endKeyIndex ? null : element.substring(metricIndex + 1)
                        )
                    );
                    continue;
                }
                if (metricIndex < 0) {
                    tokens.add(new PathElement(element, element, null, null));
                    continue;
                }
                if (metricIndex == 0 || metricIndex > element.length() - 2) {
                    throw AggregationErrors.invalidPathElement(element, path);
                }
                tuple = split(element, metricIndex, tuple);
                tokens.add(new PathElement(element, tuple[0], null, tuple[1]));
            } else {
                int keyIndex = element.lastIndexOf('[');
                if (keyIndex >= 0) {
                    if (keyIndex == 0 || keyIndex > element.length() - 3) {
                        throw AggregationErrors.invalidPathElement(element, path);
                    }
                    if (element.charAt(element.length() - 1) != ']') {
                        throw AggregationErrors.invalidPathElement(element, path);
                    }
                    tokens.add(
                        new PathElement(
                            element,
                            element.substring(0, keyIndex),
                            element.substring(keyIndex + 1, element.length() - 1),
                            null
                        )
                    );
                    continue;
                }
                tokens.add(new PathElement(element, element, null, null));
            }
        }
        return new AggregationPath(tokens);
    }

    public record PathElement(String fullName, String name, String key, String metric) {

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PathElement token = (PathElement) o;
            return Objects.equals(key, token.key) && Objects.equals(name, token.name) && Objects.equals(metric, token.metric);
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + (key != null ? key.hashCode() : 0);
            result = 31 * result + (metric != null ? metric.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return fullName;
        }
    }

    private final List<PathElement> pathElements;

    public AggregationPath(List<PathElement> tokens) {
        this.pathElements = tokens;
        if (tokens == null || tokens.size() == 0) {
            throw new IllegalArgumentException("Invalid path [" + this + "]");
        }
    }

    @Override
    public String toString() {
        return Strings.arrayToDelimitedString(pathElements.toArray(), AGG_DELIM);
    }

    public PathElement lastPathElement() {
        return pathElements.get(pathElements.size() - 1);
    }

    public List<PathElement> getPathElements() {
        return this.pathElements;
    }

    public List<String> getPathElementsAsStringList() {
        return pathElementsAsStringList(this.pathElements);
    }

    /**
     * Looks up the value of this path against a set of aggregation results.
     */
    public SortValue resolveValue(InternalAggregations aggregations) {
        try {
            Iterator<PathElement> path = pathElements.iterator();
            assert path.hasNext();
            return aggregations.sortValue(path.next(), path);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Invalid aggregation order path [" + this + "]. " + e.getMessage(), e);
        }
    }

    /**
     * Resolves the {@linkplain Aggregator} pointed to by this path against
     * the given root {@linkplain Aggregator}.
     */
    public Aggregator resolveAggregator(Aggregator root) {
        Iterator<PathElement> path = pathElements.iterator();
        assert path.hasNext();
        return root.resolveSortPathOnValidAgg(path.next(), path);
    }

    /**
     * Resolves the {@linkplain Aggregator} pointed to by the first element
     * of this path against the given root {@linkplain Aggregator}.
     */
    public Aggregator resolveTopmostAggregator(Aggregator root) {
        AggregationPath.PathElement token = pathElements.get(0);
        // TODO both unwrap and subAggregator are only used here!
        Aggregator aggregator = ProfilingAggregator.unwrap(root.subAggregator(token.name));
        assert (aggregator instanceof SingleBucketAggregator) || (aggregator instanceof NumericMetricsAggregator)
            : "this should be picked up before aggregation execution - on validate";
        return aggregator;
    }

    public BucketComparator bucketComparator(Aggregator root, SortOrder order) {
        return resolveAggregator(root).bucketComparator(Optional.ofNullable(lastPathElement().key).orElse(lastPathElement().metric), order);
    }

    private static String[] split(String toSplit, int index, String[] result) {
        result[0] = toSplit.substring(0, index);
        result[1] = toSplit.substring(index + 1);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AggregationPath other = (AggregationPath) obj;
        return pathElements.equals(other.pathElements);
    }

    @Override
    public int hashCode() {
        return pathElements.hashCode();
    }
}
