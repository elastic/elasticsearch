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

package org.elasticsearch.common.inject.spi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PrivateBinder;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Scope;
import org.elasticsearch.common.inject.Stage;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.binder.AnnotatedBindingBuilder;
import org.elasticsearch.common.inject.internal.AbstractBindingBuilder;
import org.elasticsearch.common.inject.internal.BindingBuilder;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ProviderMethodsModule;
import org.elasticsearch.common.inject.internal.SourceProvider;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Exposes elements of a module so they can be inspected or validated.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public final class Elements {

    /**
     * Records the elements executed by {@code modules}.
     */
    public static List<Element> getElements(Module... modules) {
        return getElements(Stage.DEVELOPMENT, Arrays.asList(modules));
    }

    /**
     * Records the elements executed by {@code modules}.
     */
    public static List<Element> getElements(Stage stage, Iterable<? extends Module> modules) {
        RecordingBinder binder = new RecordingBinder(stage);
        for (Module module : modules) {
            binder.install(module);
        }
        return Collections.unmodifiableList(binder.elements);
    }

    private static class RecordingBinder implements Binder, PrivateBinder {
        private final Stage stage;
        private final Set<Module> modules;
        private final List<Element> elements;
        private final Object source;
        private final SourceProvider sourceProvider;

        /**
         * The binder where exposed bindings will be created
         */
        private final RecordingBinder parent;

        private RecordingBinder(Stage stage) {
            this.stage = stage;
            this.modules = new HashSet<>();
            this.elements = new ArrayList<>();
            this.source = null;
            this.sourceProvider = new SourceProvider().plusSkippedClasses(
                Elements.class,
                RecordingBinder.class,
                AbstractModule.class,
                AbstractBindingBuilder.class,
                BindingBuilder.class
            );
            this.parent = null;
        }

        /**
         * Creates a recording binder that's backed by {@code prototype}.
         */
        private RecordingBinder(RecordingBinder prototype, Object source, SourceProvider sourceProvider) {
            if ((source == null ^ sourceProvider == null) == false) {
                throw new IllegalArgumentException();
            }

            this.stage = prototype.stage;
            this.modules = prototype.modules;
            this.elements = prototype.elements;
            this.source = source;
            this.sourceProvider = sourceProvider;
            this.parent = prototype.parent;
        }

        @Override
        public void bindScope(Class<? extends Annotation> annotationType, Scope scope) {
            elements.add(new ScopeBinding(getSource(), annotationType, scope));
        }

        @Override
        public void install(Module module) {
            if (modules.add(module)) {
                Binder binder = this;

                try {
                    module.configure(binder);
                } catch (IllegalArgumentException e) {
                    // NOTE: This is not in the original guice. We rethrow here to expose any explicit errors in configure()
                    throw e;
                } catch (RuntimeException e) {
                    Collection<Message> messages = Errors.getMessagesFromThrowable(e);
                    if (messages.isEmpty() == false) {
                        elements.addAll(messages);
                    } else {
                        addError(e);
                    }
                }
                binder.install(ProviderMethodsModule.forModule(module));
            }
        }

        @Override
        public void addError(String message, Object... arguments) {
            elements.add(new Message(getSource(), Errors.format(message, arguments)));
        }

        @Override
        public void addError(Throwable t) {
            String message = "An exception was caught and reported. Message: " + t.getMessage();
            elements.add(new Message(Collections.singletonList(getSource()), message, t));
        }

        @Override
        public void addError(Message message) {
            elements.add(message);
        }

        @Override
        public <T> AnnotatedBindingBuilder<T> bind(Key<T> key) {
            return new BindingBuilder<>(this, elements, getSource(), key);
        }

        @Override
        public <T> AnnotatedBindingBuilder<T> bind(TypeLiteral<T> typeLiteral) {
            return bind(Key.get(typeLiteral));
        }

        @Override
        public <T> AnnotatedBindingBuilder<T> bind(Class<T> type) {
            return bind(Key.get(type));
        }

        @Override
        public <T> Provider<T> getProvider(final Key<T> key) {
            final ProviderLookup<T> element = new ProviderLookup<>(getSource(), key);
            elements.add(element);
            return element.getProvider();
        }

        @Override
        public RecordingBinder withSource(final Object source) {
            return new RecordingBinder(this, source, null);
        }

        @Override
        public RecordingBinder skipSources(Class<?>... classesToSkip) {
            // if a source is specified explicitly, we don't need to skip sources
            if (source != null) {
                return this;
            }

            SourceProvider newSourceProvider = sourceProvider.plusSkippedClasses(classesToSkip);
            return new RecordingBinder(this, null, newSourceProvider);
        }

        @Override
        public void expose(Key<?> key) {
            addError("Cannot expose %s on a standard binder. " + "Exposed bindings are only applicable to private binders.", key);

        }

        private static final Logger logger = LogManager.getLogger(Elements.class);

        protected Object getSource() {
            Object ret;
            if (logger.isDebugEnabled()) {
                ret = sourceProvider != null ? sourceProvider.get() : source;
            } else {
                ret = source;
            }
            return ret == null ? "_unknown_" : ret;
        }

        @Override
        public String toString() {
            return "Binder";
        }
    }
}
