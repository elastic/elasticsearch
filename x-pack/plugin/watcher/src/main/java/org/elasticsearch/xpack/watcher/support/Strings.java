/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.support;

import java.util.Objects;

public class Strings {
    private Strings() {
    }

    public static String join(String delimiter, int... values) {
        Objects.requireNonNull(delimiter);
        Objects.requireNonNull(values);
        if (values.length == 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(4 * values.length);
        sb.append(values[0]);

        for (int i = 1; i < values.length; i++) {
            sb.append(delimiter).append(values[i]);
        }

        return sb.toString();
    }
}
