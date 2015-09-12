/**
 * Copyright (C) 2006 Google Inc.
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

package org.elasticsearch.common.inject.binder;

import org.elasticsearch.common.inject.Scope;

import java.lang.annotation.Annotation;

/**
 * See the EDSL examples at {@link org.elasticsearch.common.inject.Binder}.
 *
 * @author crazybob@google.com (Bob Lee)
 */
public interface ScopedBindingBuilder {

    /**
     * See the EDSL examples at {@link org.elasticsearch.common.inject.Binder}.
     */
    void in(Class<? extends Annotation> scopeAnnotation);

    /**
     * See the EDSL examples at {@link org.elasticsearch.common.inject.Binder}.
     */
    void in(Scope scope);

    /**
     * Instructs the {@link org.elasticsearch.common.inject.Injector} to eagerly initialize this
     * singleton-scoped binding upon creation. Useful for application
     * initialization logic.  See the EDSL examples at
     * {@link org.elasticsearch.common.inject.Binder}.
     */
    void asEagerSingleton();
}
