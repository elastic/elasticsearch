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

package org.elasticsearch.common.inject.spi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.elasticsearch.bootstrap.Bootstrap;
import org.elasticsearch.common.inject.*;
import org.elasticsearch.common.inject.binder.AnnotatedBindingBuilder;
import org.elasticsearch.common.inject.binder.AnnotatedConstantBindingBuilder;
import org.elasticsearch.common.inject.binder.AnnotatedElementBuilder;
import org.elasticsearch.common.inject.internal.*;
import org.elasticsearch.common.inject.matcher.Matcher;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.lang.annotation.Annotation;
import java.util.*;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Exposes elements of a module so they can be inspected, validated or {@link
 * Element#applyTo(Binder) rewritten}.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public final class Elements {
    private static final BindingTargetVisitor<Object, Object> GET_INSTANCE_VISITOR
            = new DefaultBindingTargetVisitor<Object, Object>() {
        @Override
        public Object visit(InstanceBinding<?> binding) {
            return binding.getInstance();
        }

        @Override
        protected Object visitOther(Binding<?> binding) {
            throw new IllegalArgumentException();
        }
    };

    /**
     * Records the elements executed by {@code modules}.
     */
    public static List<Element> getElements(Module... modules) {
        return getElements(Stage.DEVELOPMENT, Arrays.asList(modules));
    }

    /**
     * Records the elements executed by {@code modules}.
     */
    public static List<Element> getElements(Stage stage, Module... modules) {
        return getElements(stage, Arrays.asList(modules));
    }

    /**
     * Records the elements executed by {@code modules}.
     */
    public static List<Element> getElements(Iterable<? extends Module> modules) {
        return getElements(Stage.DEVELOPMENT, modules);
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

    /**
     * Returns the module composed of {@code elements}.
     */
    public static Module getModule(final Iterable<? extends Element> elements) {
        return new Module() {
            public void configure(Binder binder) {
                for (Element element : elements) {
                    element.applyTo(binder);
                }
            }
        };
    }

    @SuppressWarnings("unchecked")
    static <T> BindingTargetVisitor<T, T> getInstanceVisitor() {
        return (BindingTargetVisitor<T, T>) GET_INSTANCE_VISITOR;
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
        private final PrivateElementsImpl privateElements;

        private RecordingBinder(Stage stage) {
            this.stage = stage;
            this.modules = Sets.newHashSet();
            this.elements = Lists.newArrayList();
            this.source = null;
            this.sourceProvider = new SourceProvider().plusSkippedClasses(
                    Elements.class, RecordingBinder.class, AbstractModule.class,
                    ConstantBindingBuilderImpl.class, AbstractBindingBuilder.class, BindingBuilder.class);
            this.parent = null;
            this.privateElements = null;
        }

        /**
         * Creates a recording binder that's backed by {@code prototype}.
         */
        private RecordingBinder(
                RecordingBinder prototype, Object source, SourceProvider sourceProvider) {
            checkArgument(source == null ^ sourceProvider == null);

            this.stage = prototype.stage;
            this.modules = prototype.modules;
            this.elements = prototype.elements;
            this.source = source;
            this.sourceProvider = sourceProvider;
            this.parent = prototype.parent;
            this.privateElements = prototype.privateElements;
        }

        /**
         * Creates a private recording binder.
         */
        private RecordingBinder(RecordingBinder parent, PrivateElementsImpl privateElements) {
            this.stage = parent.stage;
            this.modules = Sets.newHashSet();
            this.elements = privateElements.getElementsMutable();
            this.source = parent.source;
            this.sourceProvider = parent.sourceProvider;
            this.parent = parent;
            this.privateElements = privateElements;
        }

        public void bindScope(Class<? extends Annotation> annotationType, Scope scope) {
            elements.add(new ScopeBinding(getSource(), annotationType, scope));
        }

        @SuppressWarnings("unchecked") // it is safe to use the type literal for the raw type
        public void requestInjection(Object instance) {
            requestInjection((TypeLiteral) TypeLiteral.get(instance.getClass()), instance);
        }

        public <T> void requestInjection(TypeLiteral<T> type, T instance) {
            elements.add(new InjectionRequest<T>(getSource(), type, instance));
        }

        public <T> MembersInjector<T> getMembersInjector(final TypeLiteral<T> typeLiteral) {
            final MembersInjectorLookup<T> element
                    = new MembersInjectorLookup<T>(getSource(), typeLiteral);
            elements.add(element);
            return element.getMembersInjector();
        }

        public <T> MembersInjector<T> getMembersInjector(Class<T> type) {
            return getMembersInjector(TypeLiteral.get(type));
        }

        public void bindListener(Matcher<? super TypeLiteral<?>> typeMatcher, TypeListener listener) {
            elements.add(new TypeListenerBinding(getSource(), listener, typeMatcher));
        }

        public void requestStaticInjection(Class<?>... types) {
            for (Class<?> type : types) {
                elements.add(new StaticInjectionRequest(getSource(), type));
            }
        }

        public void install(Module module) {
            if (modules.add(module)) {
                Binder binder = this;
                if (module instanceof PrivateModule) {
                    binder = binder.newPrivateBinder();
                }

                try {
                    module.configure(binder);
                } catch (RuntimeException e) {
                    Collection<Message> messages = Errors.getMessagesFromThrowable(e);
                    if (!messages.isEmpty()) {
                        elements.addAll(messages);
                    } else {
                        addError(e);
                    }
                }
                binder.install(ProviderMethodsModule.forModule(module));
            }
        }

        public Stage currentStage() {
            return stage;
        }

        public void addError(String message, Object... arguments) {
            elements.add(new Message(getSource(), Errors.format(message, arguments)));
        }

        public void addError(Throwable t) {
            String message = "An exception was caught and reported. Message: " + t.getMessage();
            elements.add(new Message(ImmutableList.of(getSource()), message, t));
        }

        public void addError(Message message) {
            elements.add(message);
        }

        public <T> AnnotatedBindingBuilder<T> bind(Key<T> key) {
            return new BindingBuilder<T>(this, elements, getSource(), key);
        }

        public <T> AnnotatedBindingBuilder<T> bind(TypeLiteral<T> typeLiteral) {
            return bind(Key.get(typeLiteral));
        }

        public <T> AnnotatedBindingBuilder<T> bind(Class<T> type) {
            return bind(Key.get(type));
        }

        public AnnotatedConstantBindingBuilder bindConstant() {
            return new ConstantBindingBuilderImpl<Void>(this, elements, getSource());
        }

        public <T> Provider<T> getProvider(final Key<T> key) {
            final ProviderLookup<T> element = new ProviderLookup<T>(getSource(), key);
            elements.add(element);
            return element.getProvider();
        }

        public <T> Provider<T> getProvider(Class<T> type) {
            return getProvider(Key.get(type));
        }

        public void convertToTypes(Matcher<? super TypeLiteral<?>> typeMatcher,
                                   TypeConverter converter) {
            elements.add(new TypeConverterBinding(getSource(), typeMatcher, converter));
        }

        public RecordingBinder withSource(final Object source) {
            return new RecordingBinder(this, source, null);
        }

        public RecordingBinder skipSources(Class... classesToSkip) {
            // if a source is specified explicitly, we don't need to skip sources
            if (source != null) {
                return this;
            }

            SourceProvider newSourceProvider = sourceProvider.plusSkippedClasses(classesToSkip);
            return new RecordingBinder(this, null, newSourceProvider);
        }

        public PrivateBinder newPrivateBinder() {
            PrivateElementsImpl privateElements = new PrivateElementsImpl(getSource());
            elements.add(privateElements);
            return new RecordingBinder(this, privateElements);
        }

        public void expose(Key<?> key) {
            exposeInternal(key);
        }

        public AnnotatedElementBuilder expose(Class<?> type) {
            return exposeInternal(Key.get(type));
        }

        public AnnotatedElementBuilder expose(TypeLiteral<?> type) {
            return exposeInternal(Key.get(type));
        }

        private <T> AnnotatedElementBuilder exposeInternal(Key<T> key) {
            if (privateElements == null) {
                addError("Cannot expose %s on a standard binder. "
                        + "Exposed bindings are only applicable to private binders.", key);
                return new AnnotatedElementBuilder() {
                    public void annotatedWith(Class<? extends Annotation> annotationType) {
                    }

                    public void annotatedWith(Annotation annotation) {
                    }
                };
            }

            ExposureBuilder<T> builder = new ExposureBuilder<T>(this, getSource(), key);
            privateElements.addExposureBuilder(builder);
            return builder;
        }

        private static ESLogger logger = Loggers.getLogger(Bootstrap.class);

        protected Object getSource() {
            Object ret;
            if (logger.isDebugEnabled()) {
                ret = sourceProvider != null
                        ? sourceProvider.get()
                        : source;
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
