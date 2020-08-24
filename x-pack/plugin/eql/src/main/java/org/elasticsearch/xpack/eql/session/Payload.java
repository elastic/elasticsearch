/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.common.unit.TimeValue;

import java.util.List;

/**
 * Container for internal results. Can be low-level such as SearchHits or Sequences.
 * Generalized to allow reuse and internal pluggability.
 */
public interface Payload {

    Results.Type resultType();

    boolean timedOut();

    TimeValue timeTook();

    <V> List<V> values();
}
