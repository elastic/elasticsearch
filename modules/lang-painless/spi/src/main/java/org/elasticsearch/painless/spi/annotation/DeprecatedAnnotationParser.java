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

package org.elasticsearch.painless.spi.annotation;

import java.util.Map;

public class DeprecatedAnnotationParser implements WhitelistAnnotationParser {

    public static final DeprecatedAnnotationParser INSTANCE = new DeprecatedAnnotationParser();

    public static final String MESSAGE = "message";

    private DeprecatedAnnotationParser() {

    }

    @Override
    public Object parse(Map<String, String> arguments) {
        String message = arguments.getOrDefault(MESSAGE, "");

        if ((arguments.isEmpty() || arguments.size() == 1 && arguments.containsKey(MESSAGE)) == false) {
            throw new IllegalArgumentException("unexpected parameters for [@deprecation] annotation, found " + arguments);
        }

        return new DeprecatedAnnotation(message);
    }
}
