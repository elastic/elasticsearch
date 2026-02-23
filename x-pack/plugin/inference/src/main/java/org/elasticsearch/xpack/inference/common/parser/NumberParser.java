/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.common.parser;

import java.util.Map;

import static org.elasticsearch.xpack.inference.common.parser.ObjectParserUtils.invalidTypeErrorMsg;

public final class NumberParser {

    /**
     * Extract an optional long from the map. JSON may produce Integer or Long, so we accept any Number.
     */
    public static Long extractLong(Map<String, Object> map, String key, String root) {
        var number = ObjectParserUtils.removeAsType(map, key, root, Number.class);

        if (number == null) {
            return null;
        }

        if (number instanceof Long == false && number instanceof Integer == false) {
            throw new IllegalArgumentException(invalidTypeErrorMsg(key, root, number, Long.class.getSimpleName()));
        }

        return number.longValue();
    }
}
