/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.parser;

import org.elasticsearch.xpack.ql.expression.Order.OrderDirection;

import java.time.ZoneId;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.eql.action.RequestDefaults.FETCH_SIZE;
import static org.elasticsearch.xpack.eql.action.RequestDefaults.FIELD_EVENT_CATEGORY;
import static org.elasticsearch.xpack.eql.action.RequestDefaults.FIELD_TIMESTAMP;
import static org.elasticsearch.xpack.eql.action.RequestDefaults.SIZE;

public class ParserParams {

    private final ZoneId zoneId;
    private String fieldEventCategory = FIELD_EVENT_CATEGORY;
    private String fieldTimestamp = FIELD_TIMESTAMP;
    private String fieldTiebreaker = null;
    private OrderDirection resultPosition = OrderDirection.ASC;
    private int size = SIZE;
    private int fetchSize = FETCH_SIZE;
    private List<Object> queryParams = emptyList();

    public ParserParams(ZoneId zoneId) {
        this.zoneId = zoneId;
    }

    public String fieldEventCategory() {
        return fieldEventCategory;
    }

    public ParserParams fieldEventCategory(String fieldEventCategory) {
        this.fieldEventCategory = fieldEventCategory;
        return this;
    }

    public String fieldTimestamp() {
        return fieldTimestamp;
    }

    public ParserParams fieldTimestamp(String fieldTimestamp) {
        this.fieldTimestamp = fieldTimestamp;
        return this;
    }

    public String fieldTiebreaker() {
        return fieldTiebreaker;
    }

    public ParserParams fieldTiebreaker(String fieldTiebreaker) {
        this.fieldTiebreaker = fieldTiebreaker;
        return this;
    }

    public OrderDirection resultPosition() {
        return resultPosition;
    }

    public ParserParams resultPosition(OrderDirection resultPosition) {
        this.resultPosition = resultPosition;
        return this;
    }

    public int size() {
        return size;
    }

    public ParserParams size(int size) {
        this.size = size;
        return this;
    }

    public int fetchSize() {
        return fetchSize;
    }

    public ParserParams fetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
        return this;
    }

    public List<Object> params() {
        return queryParams;
    }

    public ParserParams params(List<Object> params) {
        this.queryParams = params;
        return this;
    }

    public ZoneId zoneId() {
        return zoneId;
    }
}
