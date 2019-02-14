/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.proto;

import java.util.Objects;

class Utils {

    static void checkIndex(int index, int length) {
        return Objects.checkIndex(index, length);
    }
}
