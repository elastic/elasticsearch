/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.watcher.actions.throttler;

import org.elasticsearch.common.ParseField;

public final class ThrottlerField {
    public static final ParseField THROTTLE_PERIOD = new ParseField("throttle_period_in_millis");
    public static final ParseField THROTTLE_PERIOD_HUMAN = new ParseField("throttle_period");

    private ThrottlerField() {}
}
