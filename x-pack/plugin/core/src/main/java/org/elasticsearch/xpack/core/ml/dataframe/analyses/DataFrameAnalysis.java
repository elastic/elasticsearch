/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.dataframe.analyses;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.xcontent.ToXContentObject;

import java.util.Map;
import java.util.Set;

public interface DataFrameAnalysis extends ToXContentObject, NamedWriteable {

    /**
     * @return The analysis parameters as a map
     */
    Map<String, Object> getParams();

    /**
     * @return {@code true} if this analysis supports fields with categorical values (i.e. text, keyword, ip)
     */
    boolean supportsCategoricalFields();

    /**
     * @return The set of fields that analyzed documents must have for the analysis to operate
     */
    Set<String> getRequiredFields();
}
