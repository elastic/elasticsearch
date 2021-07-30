/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.watch;

import org.elasticsearch.common.xcontent.ParseField;

public final class WatchField {
    public static final ParseField TRIGGER = new ParseField("trigger");
    public static final ParseField INPUT = new ParseField("input");
    public static final ParseField CONDITION = new ParseField("condition");
    public static final ParseField ACTIONS = new ParseField("actions");
    public static final ParseField TRANSFORM = new ParseField("transform");
    public static final ParseField FOREACH = new ParseField("foreach");
    public static final ParseField MAX_ITERATIONS = new ParseField("max_iterations");
    public static final ParseField THROTTLE_PERIOD = new ParseField("throttle_period_in_millis");
    public static final ParseField THROTTLE_PERIOD_HUMAN = new ParseField("throttle_period");
    public static final ParseField METADATA = new ParseField("metadata");
    public static final ParseField STATUS = new ParseField("status");
    public static final String ALL_ACTIONS_ID = "_all";

    private WatchField() {}
}
