/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.dataframe.stats;

import org.elasticsearch.common.xcontent.ParseField;

/**
 * A collection of parse fields commonly used by stats objects
 */
public final class Fields {

    public static final ParseField TYPE = new ParseField("type");
    public static final ParseField JOB_ID = new ParseField("job_id");
    public static final ParseField TIMESTAMP = new ParseField("timestamp");

    private Fields() {}
}
