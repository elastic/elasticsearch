/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

public final class RequestDefaults {

    private RequestDefaults() {}

    public static final String FIELD_TIMESTAMP = "@timestamp";
    public static final String FIELD_EVENT_TYPE = "event_type";
    public static final String IMPLICIT_JOIN_KEY = "agent.id";

    public static int FETCH_SIZE = 50;
}
