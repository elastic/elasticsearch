/*
 * Copyright (C) 2008 Google Inc.
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

package org.elasticsearch.injection.guice.internal;

import org.elasticsearch.injection.guice.Injector;

/**
 * References a scope, either directly (as a scope instance), or indirectly (as a scope annotation).
 * The scope's eager or laziness is also exposed.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
public enum Scoping {
    /**
     * No scoping annotation has been applied. Note that this is different from {@code
     * in(Scopes.NO_SCOPE)}, where the 'NO_SCOPE' has been explicitly applied.
     */
    UNSCOPED,
    /**
     * One instance per {@link Injector}.
     */
    EAGER_SINGLETON
}
