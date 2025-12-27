/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.evaluator;

import org.elasticsearch.xpack.esql.core.type.DataType;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Interface for the concrete functionality that produces compound outputs from a single input.
 * The implementations of this interface should serve as a bridge between the ESQL engine and the domain-specific logic that produces
 * the compound outputs.
 */
public interface CompoundOutputFunction {

    /**
     * Returns an ordered map of output column names and their corresponding data types.
     * The column names must match the keys produced in the map returned by the {@link #evaluate(String)} method.
     * <p><b>
     * NOTE: the returned map and the order of its entries map must be 100% consistent across multiple invocations as it defines the
     * output schema, and because it may be invoked multiple times during query planning and execution. It is recommended to compute the
     * result at the first call and cache it for subsequent calls.
     *</b>
     * @return An ordered map where keys are output column names and values are their data types.
     */
    LinkedHashMap<String, DataType> getOutputColumns();

    /**
     * Evaluates the input and produces a compound output as a map of key-value pairs.
     * The order of the returned map is not guaranteed, thus looking up values should be done by key.
     *
     * @param input The String representation of the input to be evaluated.
     * @return A map representing the compound output, where keys are field names and values are the corresponding field values.
     */
    Map<String, Object> evaluate(String input) throws Exception;
}
