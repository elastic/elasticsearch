/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.job.results;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xcontent.ParseField;

/**
 * A wrapper for concrete result objects plus meta information.
 * Also contains common attributes for results.
 */
public class Result<T> {

    /**
     * Serialisation fields
     */
    public static final ParseField TYPE = new ParseField("result");
    public static final ParseField RESULT_TYPE = new ParseField("result_type");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");
    public static final ParseField IS_INTERIM = new ParseField("is_interim");

    @Nullable
    public final String index;
    @Nullable
    public final T result;

    public Result(String index, T result) {
        this.index = index;
        this.result = result;
    }
}
