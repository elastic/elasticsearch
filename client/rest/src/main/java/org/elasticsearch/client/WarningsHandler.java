/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import java.util.List;

/**
 * Called if there are warnings to determine if those warnings should fail the
 * request.
 */
public interface WarningsHandler {
    boolean warningsShouldFailRequest(List<String> warnings);

    WarningsHandler PERMISSIVE = new WarningsHandler() {
        @Override
        public boolean warningsShouldFailRequest(List<String> warnings) {
            return false;
        }

        @Override
        public String toString() {
            return "permissive";
        }
    };
    WarningsHandler STRICT = new WarningsHandler() {
        @Override
        public boolean warningsShouldFailRequest(List<String> warnings) {
            return false == warnings.isEmpty();
        }

        @Override
        public String toString() {
            return "strict";
        }
    };
}
