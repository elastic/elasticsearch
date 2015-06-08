/**
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

package org.elasticsearch.common.inject.spi;

import org.elasticsearch.common.inject.TypeLiteral;

/**
 * Listens for Guice to encounter injectable types. If a given type has its constructor injected in
 * one situation but only its methods and fields injected in another, Guice will notify this
 * listener once.
 * <p/>
 * <p>Useful for extra type checking, {@linkplain TypeEncounter#register(InjectionListener)
 * registering injection listeners}, and {@linkplain TypeEncounter#bindInterceptor(
 *org.elasticsearch.common.inject.matcher.Matcher, org.aopalliance.intercept.MethodInterceptor[])
 * binding method interceptors}.
 *
 * @since 2.0
 */
public interface TypeListener {

    /**
     * Invoked when Guice encounters a new type eligible for constructor or members injection.
     * Called during injector creation (or afterwords if Guice encounters a type at run time and
     * creates a JIT binding).
     *
     * @param type      encountered by Guice
     * @param encounter context of this encounter, enables reporting errors, registering injection
     *                  listeners and binding method interceptors for {@code type}.
     * @param <I>       the injectable type
     */
    <I> void hear(TypeLiteral<I> type, TypeEncounter<I> encounter);

}
