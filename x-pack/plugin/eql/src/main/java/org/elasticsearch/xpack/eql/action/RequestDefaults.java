/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.action;

public final class RequestDefaults {

    private RequestDefaults() {}

    public static final String FIELD_TIMESTAMP = "@timestamp";
    public static final String FIELD_EVENT_CATEGORY = "event.category";

    public static int SIZE = 10;
    public static int FETCH_SIZE = 1000;
    public static boolean CCS_MINIMIZE_ROUNDTRIPS = true;
    public static int MAX_SAMPLES_PER_KEY = 1;
}
