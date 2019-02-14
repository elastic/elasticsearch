/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import java.util.Locale;

import static java.lang.String.format;

class Utils {

    static void checkIndex(int index, int length) {
        if (index < 0 || index >= length) {
            throw new IndexOutOfBoundsException(format(Locale.ROOT, "Index out of range:  %d (max is %d)", index, length - 1));
        }
    }
}
