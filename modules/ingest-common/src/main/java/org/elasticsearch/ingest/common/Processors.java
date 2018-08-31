/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
