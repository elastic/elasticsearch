/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.support;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.HasAggregations;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregator;
import org.elasticsearch.search.aggregations.metrics.InternalNumericMetricsAggregation;
import org.elasticsearch.search.aggregations.metrics.NumericMetricsAggregator;

import java.util.ArrayList;
import java.util.List;

/**
 * A path that can be used to sort/order buckets (in some multi-bucket aggregations, eg terms & histogram) based on
 * sub-aggregations. The path may point to either a single-bucket aggregation or a metrics aggregation. If the path
 * points to a single-bucket aggregation, the sort will be applied based on the {@code doc_count} of the bucket. If this
 * path points to a metrics aggregation, if it's a single-value metrics (eg. avg, max, min, etc..) the sort will be
 * applied on that single value. If it points to a multi-value metrics, the path should point out what metric should be
 * the sort-by value.
 * <p/>
 * The path has the following form:
 * <p/>
 * <center>{@code <aggregation_name>['>'<aggregation_name>*]['.'<metric_name>]}</center>
 * <p/>
 * <p/>
 * Examples:
 *
 * <ul>
 *     <li>
 *         {@code agg1>agg2>agg3} - where agg1, agg2 and agg3 are all single-bucket aggs (eg filter, nested, missing, etc..). In
 *                                  this case, the order will be based on the number of documents under {@code agg3}.
 *     </li>
 *     <li>
 *         {@code agg1>agg2>agg3} - where agg1 and agg2 are both single-bucket aggs and agg3 is a single-value metrics agg (eg avg, max, min, etc..).
 *                                  In this case, the order will be based on the value of {@code agg3}.
 *     </li>
 *     <li>
 *         {@code agg1>agg2>agg3.avg} - where agg1 and agg2 are both single-bucket aggs and agg3 is a multi-value metrics agg (eg stats, extended_stats, etc...).
 *                                  In this case, the order will be based on the avg value of {@code agg3}.
 *     </li>
 * </ul>
 *
 */
public class AggregationPath {

    private final static String AGG_DELIM = ">";

    public static AggregationPath parse(String path) {
        String[] elements = Strings.tokenizeToStringArray(path, AGG_DELIM);
        List<PathElement> tokens = new ArrayList<>(elements.length);
        String[] tuple = new String[2];
        for (int i = 0; i < elements.length; i++) {
            String element = elements[i];
            if (i == elements.length - 1) {
                int index = element.lastIndexOf('[');
                if (index >= 0) {
                    if (index == 0 || index > element.length() - 3) {
                        throw new AggregationExecutionException("Invalid path element [" + element + "] in path [" + path + "]");
                    }
                    if (element.charAt(element.length() - 1) != ']') {
                        throw new AggregationExecutionException("Invalid path element [" + element + "] in path [" + path + "]");
                    }
                    tokens.add(new PathElement(element, element.substring(0, index), element.substring(index + 1, element.length() - 1)));
                    continue;
                }
                index = element.lastIndexOf('.');
                if (index < 0) {
                    tokens.add(new PathElement(element, element, null));
                    continue;
                }
                if (index == 0 || index > element.length() - 2) {
                    throw new AggregationExecutionException("Invalid path element [" + element + "] in path [" + path + "]");
                }
                tuple = split(element, index, tuple);
                tokens.add(new PathElement(element, tuple[0], tuple[1]));

            } else {
                int index = element.lastIndexOf('[');
                if (index >= 0) {
                    if (index == 0 || index > element.length() - 3) {
                        throw new AggregationExecutionException("Invalid path element [" + element + "] in path [" + path + "]");
                    }
                    if (element.charAt(element.length() - 1) != ']') {
                        throw new AggregationExecutionException("Invalid path element [" + element + "] in path [" + path + "]");
                    }
                    tokens.add(new PathElement(element, element.substring(0, index), element.substring(index + 1, element.length() - 1)));
                    continue;
                }
                tokens.add(new PathElement(element, element, null));
            }
        }
        return new AggregationPath(tokens);
    }

    public static class PathElement {

        private final String fullName;
        public final String name;
        public final String key;

        public PathElement(String fullName, String name, String key) {
            this.fullName = fullName;
            this.name = name;
            this.key = key;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            PathElement token = (PathElement) o;

            if (key != null ? !key.equals(token.key) : token.key != null) return false;
            if (!name.equals(token.name)) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = name.hashCode();
            result = 31 * result + (key != null ? key.hashCode() : 0);
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
            throw new ElasticsearchIllegalArgumentException("Invalid path [" + this + "]");
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
        List<String> stringPathElements = new ArrayList<>();
        for (PathElement pathElement : this.pathElements) {
            stringPathElements.add(pathElement.name);
            if (pathElement.key != null) {
                stringPathElements.add(pathElement.key);
            }
        }
        return stringPathElements;
    }

    public AggregationPath subPath(int offset, int length) {
        PathElement[] subTokens = new PathElement[length];
        System.arraycopy(pathElements, offset, subTokens, 0, length);
        return new AggregationPath(pathElements);
    }

    /**
     * Resolves the value pointed by this path given an aggregations root
     *
     * @param root  The root that serves as a point of reference for this path
     * @return      The resolved value
     */
    public double resolveValue(HasAggregations root) {
        HasAggregations parent = root;
        double value = Double.NaN;
        for (int i = 0; i < pathElements.size(); i++) {
            AggregationPath.PathElement token = pathElements.get(i);
            Aggregation agg = parent.getAggregations().get(token.name);

            if (agg == null) {
                throw new ElasticsearchIllegalArgumentException("Invalid order path [" + this +
                        "]. Cannot find aggregation named [" + token.name + "]");
            }

            if (agg instanceof SingleBucketAggregation) {
                if (token.key != null && !token.key.equals("doc_count")) {
                    throw new ElasticsearchIllegalArgumentException("Invalid order path [" + this +
                            "]. Unknown value key [" + token.key + "] for single-bucket aggregation [" + token.name +
                            "]. Either use [doc_count] as key or drop the key all together");
                }
                parent = (SingleBucketAggregation) agg;
                value = ((SingleBucketAggregation) agg).getDocCount();
                continue;
            }

            // the agg can only be a metrics agg, and a metrics agg must be at the end of the path
            if (i != pathElements.size() - 1) {
                throw new ElasticsearchIllegalArgumentException("Invalid order path [" + this +
 "]. Metrics aggregations cannot have sub-aggregations (at [" + token + ">" + pathElements.get(i + 1) + "]");
            }

            if (agg instanceof InternalNumericMetricsAggregation.SingleValue) {
                if (token.key != null && !token.key.equals("value")) {
                    throw new ElasticsearchIllegalArgumentException("Invalid order path [" + this +
                            "]. Unknown value key [" + token.key + "] for single-value metric aggregation [" + token.name +
                            "]. Either use [value] as key or drop the key all together");
                }
                parent = null;
                value = ((InternalNumericMetricsAggregation.SingleValue) agg).value();
                continue;
            }

            // we're left with a multi-value metric agg
            if (token.key == null) {
                throw new ElasticsearchIllegalArgumentException("Invalid order path [" + this +
                        "]. Missing value key in [" + token + "] which refers to a multi-value metric aggregation");
            }
            parent = null;
            value = ((InternalNumericMetricsAggregation.MultiValue) agg).value(token.key);
        }

        return value;
    }

    /**
     * Resolves the aggregator pointed by this path using the given root as a point of reference.
     *
     * @param root      The point of reference of this path
     * @return          The aggregator pointed by this path starting from the given aggregator as a point of reference
     */
    public Aggregator resolveAggregator(Aggregator root) {
        Aggregator aggregator = root;
        for (int i = 0; i < pathElements.size(); i++) {
            AggregationPath.PathElement token = pathElements.get(i);
            aggregator = aggregator.subAggregator(token.name);
            assert (aggregator instanceof SingleBucketAggregator && i <= pathElements.size() - 1)
                    || (aggregator instanceof NumericMetricsAggregator && i == pathElements.size() - 1) :
                    "this should be picked up before aggregation execution - on validate";
        }
        return aggregator;
    }
    
    /**
     * Resolves the topmost aggregator pointed by this path using the given root as a point of reference.
     *
     * @param root      The point of reference of this path
     * @return          The first child aggregator of the root pointed by this path 
     */
    public Aggregator resolveTopmostAggregator(Aggregator root) {
        AggregationPath.PathElement token = pathElements.get(0);
        Aggregator aggregator = root.subAggregator(token.name);
        assert (aggregator instanceof SingleBucketAggregator )
                || (aggregator instanceof NumericMetricsAggregator) : "this should be picked up before aggregation execution - on validate";
        return aggregator;
    }    

    /**
     * Validates this path over the given aggregator as a point of reference.
     *
     * @param root  The point of reference of this path
     */
    public void validate(Aggregator root) {
        Aggregator aggregator = root;
        for (int i = 0; i < pathElements.size(); i++) {
            aggregator = aggregator.subAggregator(pathElements.get(i).name);
            if (aggregator == null) {
                throw new AggregationExecutionException("Invalid term-aggregator order path [" + this + "]. Unknown aggregation ["
                        + pathElements.get(i).name + "]");
            }
            if (i < pathElements.size() - 1) {

                // we're in the middle of the path, so the aggregator can only be a single-bucket aggregator

                if (!(aggregator instanceof SingleBucketAggregator)) {
                    throw new AggregationExecutionException("Invalid terms aggregation order path [" + this +
                            "]. Terms buckets can only be sorted on a sub-aggregator path " +
                            "that is built out of zero or more single-bucket aggregations within the path and a final " +
                            "single-bucket or a metrics aggregation at the path end. Sub-path [" +
                            subPath(0, i + 1) + "] points to non single-bucket aggregation");
                }

                if (pathElements.get(i).key != null) {
                    throw new AggregationExecutionException("Invalid terms aggregation order path [" + this +
                            "]. Terms buckets can only be sorted on a sub-aggregator path " +
                            "that is built out of zero or more single-bucket aggregations within the path and a " +
                            "final single-bucket or a metrics aggregation at the path end. Sub-path [" +
                            subPath(0, i + 1) + "] points to non single-bucket aggregation");
                }
            }
        }
        boolean singleBucket = aggregator instanceof SingleBucketAggregator;
        if (!singleBucket && !(aggregator instanceof NumericMetricsAggregator)) {
            throw new AggregationExecutionException("Invalid terms aggregation order path [" + this +
                    "]. Terms buckets can only be sorted on a sub-aggregator path " +
                    "that is built out of zero or more single-bucket aggregations within the path and a final " +
                    "single-bucket or a metrics aggregation at the path end.");
        }

        AggregationPath.PathElement lastToken = lastPathElement();

        if (singleBucket) {
            if (lastToken.key != null && !"doc_count".equals(lastToken.key)) {
                throw new AggregationExecutionException("Invalid terms aggregation order path [" + this +
                        "]. Ordering on a single-bucket aggregation can only be done on its doc_count. " +
                        "Either drop the key (a la \"" + lastToken.name + "\") or change it to \"doc_count\" (a la \"" + lastToken.name + ".doc_count\")");
            }
            return;   // perfectly valid to sort on single-bucket aggregation (will be sored on its doc_count)
        }

        if (aggregator instanceof NumericMetricsAggregator.SingleValue) {
            if (lastToken.key != null && !"value".equals(lastToken.key)) {
                throw new AggregationExecutionException("Invalid terms aggregation order path [" + this +
                        "]. Ordering on a single-value metrics aggregation can only be done on its value. " +
                        "Either drop the key (a la \"" + lastToken.name + "\") or change it to \"value\" (a la \"" + lastToken.name + ".value\")");
            }
            return;   // perfectly valid to sort on single metric aggregation (will be sorted on its associated value)
        }

        // the aggregator must be of a multi-value metrics type
        if (lastToken.key == null) {
            throw new AggregationExecutionException("Invalid terms aggregation order path [" + this +
                    "]. When ordering on a multi-value metrics aggregation a metric name must be specified");
        }

        if (!((NumericMetricsAggregator.MultiValue) aggregator).hasMetric(lastToken.key)) {
            throw new AggregationExecutionException("Invalid terms aggregation order path [" + this +
                    "]. Unknown metric name [" + lastToken.key + "] on multi-value metrics aggregation [" + lastToken.name + "]");
        }
    }

    private static String[] split(String toSplit, int index, String[] result) {
        result[0] = toSplit.substring(0, index);
        result[1] = toSplit.substring(index + 1);
        return result;
    }
}
