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

import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.MembersInjector;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.TypeLiteral;

/**
 * Context of an injectable type encounter. Enables reporting errors, registering injection
 * listeners and binding method interceptors for injectable type {@code I}. It is an error to use
 * an encounter after the {@link TypeListener#hear(TypeLiteral, TypeEncounter) hear()} method has
 * returned.
 *
 * @param <I> the injectable type encountered
 * @since 2.0
 */
@SuppressWarnings("overloads")
public interface TypeEncounter<I> {

    /**
     * Records an error message for type {@code I} which will be presented to the user at a later
     * time. Unlike throwing an exception, this enable us to continue configuring the Injector and
     * discover more errors. Uses {@link String#format(String, Object[])} to insert the arguments
     * into the message.
     */
    void addError(String message, Object... arguments);

    /**
     * Records an exception for type {@code I}, the full details of which will be logged, and the
     * message of which will be presented to the user at a later time. If your type listener calls
     * something that you worry may fail, you should catch the exception and pass it to this method.
     */
    void addError(Throwable t);

    /**
     * Records an error message to be presented to the user at a later time.
     */
    void addError(Message message);

    /**
     * Returns the provider used to obtain instances for the given injection key. The returned
     * provider will not be valid until the injector has been created. The provider will throw an
     * {@code IllegalStateException} if you try to use it beforehand.
     */
    <T> Provider<T> getProvider(Key<T> key);

    /**
     * Returns the provider used to obtain instances for the given injection type. The returned
     * provider will not be valid until the injetor has been created. The provider will throw an
     * {@code IllegalStateException} if you try to use it beforehand.
     */
    <T> Provider<T> getProvider(Class<T> type);

    /**
     * Returns the members injector used to inject dependencies into methods and fields on instances
     * of the given type {@code T}. The returned members injector will not be valid until the main
     * injector has been created. The members injector will throw an {@code IllegalStateException}
     * if you try to use it beforehand.
     *
     * @param typeLiteral type to get members injector for
     */
    <T> MembersInjector<T> getMembersInjector(TypeLiteral<T> typeLiteral);

    /**
     * Returns the members injector used to inject dependencies into methods and fields on instances
     * of the given type {@code T}. The returned members injector will not be valid until the main
     * injector has been created. The members injector will throw an {@code IllegalStateException}
     * if you try to use it beforehand.
     *
     * @param type type to get members injector for
     */
    <T> MembersInjector<T> getMembersInjector(Class<T> type);

    /**
     * Registers a members injector for type {@code I}. Guice will use the members injector after its
     * performed its own injections on an instance of {@code I}.
     */
    void register(MembersInjector<? super I> membersInjector);

    /**
     * Registers an injection listener for type {@code I}. Guice will notify the listener after all
     * injections have been performed on an instance of {@code I}.
     */
    void register(InjectionListener<? super I> listener);
}
