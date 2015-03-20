/**
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

package org.elasticsearch.common.inject;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.spi.InjectionPoint;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages and injects instances at injector-creation time. This is made more complicated by
 * instances that request other instances while they're being injected. We overcome this by using
 * {@link Initializable}, which attempts to perform injection before use.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
class Initializer {
    /**
     * the only thread that we'll use to inject members.
     */
    private final Thread creatingThread = Thread.currentThread();

    /**
     * zero means everything is injected.
     */
    private final CountDownLatch ready = new CountDownLatch(1);

    /**
     * Maps instances that need injection to a source that registered them
     */
    private final Map<Object, InjectableReference<?>> pendingInjection = Maps.newIdentityHashMap();

    /**
     * Registers an instance for member injection when that step is performed.
     *
     * @param instance an instance that optionally has members to be injected (each annotated with
     * @param source   the source location that this injection was requested
     * @Inject).
     */
    public <T> Initializable<T> requestInjection(InjectorImpl injector, T instance, Object source,
                                                 Set<InjectionPoint> injectionPoints) {
        checkNotNull(source);

        // short circuit if the object has no injections
        if (instance == null
                || (injectionPoints.isEmpty() && !injector.membersInjectorStore.hasTypeListeners())) {
            return Initializables.of(instance);
        }

        InjectableReference<T> initializable = new InjectableReference<>(injector, instance, source);
        pendingInjection.put(instance, initializable);
        return initializable;
    }

    /**
     * Prepares member injectors for all injected instances. This prompts Guice to do static analysis
     * on the injected instances.
     */
    void validateOustandingInjections(Errors errors) {
        for (InjectableReference<?> reference : pendingInjection.values()) {
            try {
                reference.validate(errors);
            } catch (ErrorsException e) {
                errors.merge(e.getErrors());
            }
        }
    }

    /**
     * Performs creation-time injections on all objects that require it. Whenever fulfilling an
     * injection depends on another object that requires injection, we inject it first. If the two
     * instances are codependent (directly or transitively), ordering of injection is arbitrary.
     */
    void injectAll(final Errors errors) {
        // loop over a defensive copy since ensureInjected() mutates the set. Unfortunately, that copy
        // is made complicated by a bug in IBM's JDK, wherein entrySet().toArray(Object[]) doesn't work
        for (InjectableReference<?> reference : Lists.newArrayList(pendingInjection.values())) {
            try {
                reference.get(errors);
            } catch (ErrorsException e) {
                errors.merge(e.getErrors());
            }
        }

        if (!pendingInjection.isEmpty()) {
            throw new AssertionError("Failed to satisfy " + pendingInjection);
        }

        ready.countDown();
    }

    private class InjectableReference<T> implements Initializable<T> {
        private final InjectorImpl injector;
        private final T instance;
        private final Object source;
        private MembersInjectorImpl<T> membersInjector;

        public InjectableReference(InjectorImpl injector, T instance, Object source) {
            this.injector = injector;
            this.instance = checkNotNull(instance, "instance");
            this.source = checkNotNull(source, "source");
        }

        public void validate(Errors errors) throws ErrorsException {
            @SuppressWarnings("unchecked") // the type of 'T' is a TypeLiteral<T>
                    TypeLiteral<T> type = TypeLiteral.get((Class<T>) instance.getClass());
            membersInjector = injector.membersInjectorStore.get(type, errors.withSource(source));
        }

        /**
         * Reentrant. If {@code instance} was registered for injection at injector-creation time, this
         * method will ensure that all its members have been injected before returning.
         */
        @Override
        public T get(Errors errors) throws ErrorsException {
            if (ready.getCount() == 0) {
                return instance;
            }

            // just wait for everything to be injected by another thread
            if (Thread.currentThread() != creatingThread) {
                try {
                    ready.await();
                    return instance;
                } catch (InterruptedException e) {
                    // Give up, since we don't know if our injection is ready
                    throw new RuntimeException(e);
                }
            }

            // toInject needs injection, do it right away. we only do this once, even if it fails
            if (pendingInjection.remove(instance) != null) {
                membersInjector.injectAndNotify(instance, errors.withSource(source));
            }

            return instance;
        }

        @Override
        public String toString() {
            return instance.toString();
        }
    }
}