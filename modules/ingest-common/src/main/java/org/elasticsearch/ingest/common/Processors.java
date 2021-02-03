/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.common;

import java.util.Map;

public final class Processors {

    public static long bytes(String value) {
        return BytesProcessor.apply(value);
    }

    public static String lowercase(String value) {
        return LowercaseProcessor.apply(value);
    }

    public static String uppercase(String value) {
        return UppercaseProcessor.apply(value);
    }

    public static Object json(Object fieldValue) {
        return JsonProcessor.apply(fieldValue);
    }

    public static void json(Map<String, Object> ctx, String field) {
        JsonProcessor.apply(ctx, field);
    }

    public static String urlDecode(String value) {
        return URLDecodeProcessor.apply(value);
    }

}
