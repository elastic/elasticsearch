/*
 * Copyright (C) 2009 Google Inc.
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

package org.elasticsearch.common.inject.internal;

/**
 * Thrown when a computer function returns null. This subclass exists so
 * that our ReferenceCache adapter can differentiate null output from null
 * keys, but we don't want to make this public otherwise.
 *
 * @author Bob Lee
 */
class NullOutputException extends NullPointerException {
    NullOutputException(String s) {
        super(s);
    }
}
