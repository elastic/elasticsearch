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

package org.elasticsearch.common;

import java.util.Locale;

public class Preconditions {
    private Preconditions() {
    }

    public static void checkArgument(boolean expression) {
        if (!expression) {
            throw new IllegalArgumentException();
        }
    }

    public static void checkArgument(boolean expression, @Nullable Object errorMessage) {
        if (!expression) {
            throw new IllegalArgumentException(String.valueOf(errorMessage));
        }
    }

    public static void checkArgument(boolean expression, @Nullable String errorMessageTemplate, @Nullable Object... errorMessageArgs) {
        if (!expression) {
            throw new IllegalArgumentException(format(errorMessageTemplate, errorMessageArgs));
        }
    }

    public static void checkState(boolean expression) {
        if (!expression) {
            throw new IllegalStateException();
        }
    }

    public static void checkState(boolean expression, @Nullable Object errorMessage) {
        if (!expression) {
            throw new IllegalStateException(String.valueOf(errorMessage));
        }
    }

    public static void checkState(boolean expression, @Nullable String errorMessageTemplate, @Nullable Object... errorMessageArgs) {
        if (!expression) {
            throw new IllegalStateException(format(errorMessageTemplate, errorMessageArgs));
        }
    }

    private static String format(@Nullable String errorMessageTemplate, @Nullable Object... errorMessageArgs) {
        return String.format(Locale.ROOT, errorMessageTemplate, errorMessageArgs);
    }
}
