/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.utils;

public final class OutputFieldNameConverter {

    private OutputFieldNameConverter() {}

    public static String fromDouble(double d) {
        if (d == (long) d) return String.valueOf((long) d);
        else return String.valueOf(d).replace('.', '_');
    }
}
