/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.action;

public final class RequestDefaults {

    private RequestDefaults() {}

    public static final String FIELD_TIMESTAMP = "@timestamp";
    public static final String FIELD_EVENT_CATEGORY = "event.category";

    public static int SIZE = 10;
    public static int FETCH_SIZE = 1000;
}
