/*
 * Copyright (C) 2007 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject;

/**
 * Thrown from {@link Provider#get} when an attempt is made to access a scoped
 * object while the scope in question is not currently active.
 *
 * @author kevinb@google.com (Kevin Bourrillion)
 * @since 2.0
 */
public final class OutOfScopeException extends RuntimeException {

    public OutOfScopeException(String message) {
        super(message);
    }

    public OutOfScopeException(String message, Throwable cause) {
        super(message, cause);
    }

    public OutOfScopeException(Throwable cause) {
        super(cause);
    }
}
