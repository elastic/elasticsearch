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

package org.elasticsearch.injection.guice;

import org.elasticsearch.injection.guice.internal.Errors;
import org.elasticsearch.injection.guice.internal.InternalContext;
import org.elasticsearch.injection.guice.internal.InternalFactory;
import org.elasticsearch.injection.guice.internal.ProviderInstanceBindingImpl;
import org.elasticsearch.injection.guice.internal.Scoping;
import org.elasticsearch.injection.guice.internal.SourceProvider;
import org.elasticsearch.injection.guice.internal.Stopwatch;
import org.elasticsearch.injection.guice.spi.Dependency;
import org.elasticsearch.injection.guice.spi.Element;
import org.elasticsearch.injection.guice.spi.Elements;
import org.elasticsearch.injection.guice.spi.InjectionPoint;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static java.util.Collections.emptySet;

/**
 * A partially-initialized injector. See {@link InjectorBuilder}, which uses this to build a tree
 * of injectors in batch.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
class InjectorShell {

    private final List<Element> elements;
    private final InjectorImpl injector;

    private InjectorShell(List<Element> elements, InjectorImpl injector) {
        this.elements = elements;
        this.injector = injector;
    }

    InjectorImpl getInjector() {
        return injector;
    }

    List<Element> getElements() {
        return elements;
    }

    static class Builder {
        private final List<Element> elements = new ArrayList<>();
        private final List<Module> modules = new ArrayList<>();

        /**
         * lazily constructed
         */
        private State state;

        void addModules(Iterable<? extends Module> modules) {
            for (Module module : modules) {
                this.modules.add(module);
            }
        }

        /**
         * Synchronize on this before calling {@link #build}.
         */
        Object lock() {
            return getState().lock();
        }

        /**
         * Creates and returns the injector shells for the current modules.
         */
        List<InjectorShell> build(BindingProcessor bindingProcessor, Stopwatch stopwatch, Errors errors) {
            if (state == null) {
                throw new IllegalStateException("no state. Did you remember to lock() ?");
            }

            InjectorImpl injector = new InjectorImpl(state);

            // bind Stage and Singleton if this is a top-level injector
            modules.add(0, new RootModule());
            new TypeConverterBindingProcessor(errors).prepareBuiltInConverters(injector);

            elements.addAll(Elements.getElements(modules));
            stopwatch.resetAndLog("Module execution");

            new MessageProcessor(errors).process(injector, elements);

            injector.membersInjectorStore = new MembersInjectorStore(injector);
            stopwatch.resetAndLog("TypeListeners creation");

            stopwatch.resetAndLog("Scopes creation");

            new TypeConverterBindingProcessor(errors).process(injector, elements);
            stopwatch.resetAndLog("Converters creation");

            bindInjector(injector);
            bindLogger(injector);
            bindingProcessor.process(injector, elements);
            stopwatch.resetAndLog("Binding creation");

            List<InjectorShell> injectorShells = new ArrayList<>();
            injectorShells.add(new InjectorShell(elements, injector));

            stopwatch.resetAndLog("Private environment creation");

            return injectorShells;
        }

        private State getState() {
            if (state == null) {
                state = new InheritingState();
            }
            return state;
        }
    }

    /**
     * The Injector is a special case because we allow both parent and child injectors to both have
     * a binding for that key.
     */
    private static void bindInjector(InjectorImpl injector) {
        Key<Injector> key = Key.get(Injector.class);
        InjectorFactory injectorFactory = new InjectorFactory(injector);
        injector.state.putBinding(
            key,
            new ProviderInstanceBindingImpl<>(
                injector,
                key,
                SourceProvider.UNKNOWN_SOURCE,
                injectorFactory,
                Scoping.UNSCOPED,
                injectorFactory,
                emptySet()
            )
        );
    }

    private static class InjectorFactory implements InternalFactory<Injector>, Provider<Injector> {
        private final Injector injector;

        private InjectorFactory(Injector injector) {
            this.injector = injector;
        }

        @Override
        public Injector get(Errors errors, InternalContext context, Dependency<?> dependency) {
            return injector;
        }

        @Override
        public Injector get() {
            return injector;
        }

        @Override
        public String toString() {
            return "Provider<Injector>";
        }
    }

    /**
     * The Logger is a special case because it knows the injection point of the injected member. It's
     * the only binding that does this.
     */
    private static void bindLogger(InjectorImpl injector) {
        Key<Logger> key = Key.get(Logger.class);
        LoggerFactory loggerFactory = new LoggerFactory();
        injector.state.putBinding(
            key,
            new ProviderInstanceBindingImpl<>(
                injector,
                key,
                SourceProvider.UNKNOWN_SOURCE,
                loggerFactory,
                Scoping.UNSCOPED,
                loggerFactory,
                emptySet()
            )
        );
    }

    private static class LoggerFactory implements InternalFactory<Logger>, Provider<Logger> {
        @Override
        public Logger get(Errors errors, InternalContext context, Dependency<?> dependency) {
            InjectionPoint injectionPoint = dependency.getInjectionPoint();
            return injectionPoint == null
                ? Logger.getAnonymousLogger()
                : Logger.getLogger(injectionPoint.getMember().getDeclaringClass().getName());
        }

        @Override
        public Logger get() {
            return Logger.getAnonymousLogger();
        }

        @Override
        public String toString() {
            return "Provider<Logger>";
        }
    }

    private static class RootModule implements Module {
        private RootModule() {}

        @Override
        public void configure(Binder binder) {
            binder.withSource(SourceProvider.UNKNOWN_SOURCE);
        }
    }
}
