/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support.clock;

import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.unit.TimeValue;

/**
 *
 */
public interface Clock {

    long millis();

    long nanos();

    DateTime now();

    TimeValue timeElapsedSince(DateTime time);

}
