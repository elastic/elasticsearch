/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.inference.trainedmodel.ensemble;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.xpack.core.ml.utils.NamedXContentObject;

import java.util.List;

public interface OutputAggregator extends NamedXContentObject, NamedWriteable, Accountable {

    /**
     * @return The expected size of the values array when aggregating. `null` implies there is no expected size.
     */
    Integer expectedValueSize();

    /**
     * This pre-processes the values so that they may be passed directly to the {@link OutputAggregator#aggregate(List)} method.
     *
     * Two major types of pre-processed values could be returned:
     *   - The confidence/probability scaled values given the input values (See: {@link WeightedMode#processValues(List)}
     *   - A simple transformation of the passed values in preparation for aggregation (See: {@link WeightedSum#processValues(List)}
     * @param values the values to process
     * @return A new list containing the processed values or the same list if no processing is required
     */
    List<Double> processValues(List<Double> values);

    /**
     * Function to aggregate the processed values into a single double
     *
     * This may be as simple as returning the index of the maximum value.
     *
     * Or as complex as a mathematical reduction of all the passed values (i.e. summation, average, etc.).
     *
     * @param processedValues The values to aggregate
     * @return the aggregated value.
     */
    double aggregate(List<Double> processedValues);

    /**
     * @return The name of the output aggregator
     */
    String getName();

    boolean compatibleWith(TargetType targetType);
}
