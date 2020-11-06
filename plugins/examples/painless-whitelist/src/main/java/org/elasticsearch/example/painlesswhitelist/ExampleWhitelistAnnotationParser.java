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

package org.elasticsearch.example.painlesswhitelist;

import org.elasticsearch.painless.spi.annotation.WhitelistAnnotationParser;

import java.util.Map;

public class ExampleWhitelistAnnotationParser implements WhitelistAnnotationParser {

    public static final ExampleWhitelistAnnotationParser INSTANCE = new ExampleWhitelistAnnotationParser();

    private ExampleWhitelistAnnotationParser() {

    }

    @Override
    public Object parse(Map<String, String> arguments) {
        if (arguments.size() != 2) {
            throw new IllegalArgumentException("expected exactly two arguments");
        }

        String categoryString = arguments.get("category");

        if (categoryString == null) {
            throw new IllegalArgumentException("expected category argument");
        }

        int category;

        try {
            category = Integer.parseInt(categoryString);
        } catch (NumberFormatException nfe) {
            throw new IllegalArgumentException("expected category as an int, found [" + categoryString + "]", nfe);
        }

        String message = arguments.get("message");

        if (categoryString == null) {
            throw new IllegalArgumentException("expected message argument");
        }

        return new ExamplePainlessAnnotation(category, message);
    }
}
