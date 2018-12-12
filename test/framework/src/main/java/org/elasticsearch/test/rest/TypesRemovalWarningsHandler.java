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

package org.elasticsearch.test.rest;

import org.elasticsearch.client.WarningsHandler;

import java.util.List;

/**
 * An implementation of {@link WarningsHandler} that ignores warnings related to types removal,
 * but fails the request on all other warnings.
 */
public class TypesRemovalWarningsHandler implements WarningsHandler {

    public static final TypesRemovalWarningsHandler INSTANCE = new TypesRemovalWarningsHandler();

    private TypesRemovalWarningsHandler() {
        // Prevent instantiation.
    }

    @Override
    public boolean warningsShouldFailRequest(List<String> warnings) {
        for (String warning : warnings) {
            if (warning.startsWith("[types removal]") == false) {
                return true;
            }
        }
        return false;
    }
}
