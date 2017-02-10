/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.results;

import org.elasticsearch.common.ParseField;

/**
 * Common attributes of the result types
 */
public class Result {

    /**
     * Serialisation fields
     */
    public static final ParseField TYPE = new ParseField("result");
    public static final ParseField RESULT_TYPE = new ParseField("result_type");
}
