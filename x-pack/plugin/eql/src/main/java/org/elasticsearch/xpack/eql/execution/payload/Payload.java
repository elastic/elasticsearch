/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.execution.payload;

import org.elasticsearch.common.unit.TimeValue;

import java.util.List;

public interface Payload<V> {

    boolean timedOut();

    TimeValue timeTook();

    Object[] nextKeys();

    List<V> values();
}
