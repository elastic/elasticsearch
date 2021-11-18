/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.session;

import org.elasticsearch.core.TimeValue;

import java.util.List;

/**
 * Container for final results. Used for completed data, such as Events or Sequences.
 */
public interface Payload {

    enum Type {
        EVENT,
        SEQUENCE;
    }

    Type resultType();

    boolean timedOut();

    TimeValue timeTook();

    List<?> values();
}
