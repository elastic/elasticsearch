/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.planner;

import org.elasticsearch.xpack.ql.querydsl.query.Query;

import java.time.ZoneId;

public interface Translatable {
    Query getQuery(String name, Object value, String format, boolean isDateLiteralComparison, ZoneId zoneId);
}
