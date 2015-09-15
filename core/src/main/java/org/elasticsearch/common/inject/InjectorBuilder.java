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

package org.elasticsearch.common.inject;

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.inject.internal.*;
import org.elasticsearch.common.inject.spi.Dependency;
import org.elasticsearch.common.util.iterable.Iterables;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds a tree of injectors. This is a primary injector, plus child injectors needed for each
 * {@link Binder#newPrivateBinder() private environment}. The primary injector is not necessarily a
 * top-level injector.
 * <p/>
 * <p>Injector construction happens in two phases.
 * <ol>
 * <li>Static building. In this phase, we interpret commands, create bindings, and inspect
 * dependencies. During this phase, we hold a lock to ensure consistency with parent injectors.
 * No user code is executed in this phase.</li>
 * <li>Dynamic injection. In this phase, we call user code. We inject members that requested
 * injection. This may require user's objects be created and their providers be called. And we
 * create eager singletons. In this phase, user code may have started other threads. This phase
 * is not executed for injectors created using {@link Stage#TOOL the tool stage}</li>
 * </ol>
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 */
class InjectorBuilder {

    private final Stopwatch stopwatch = new Stopwatch();
    private final Errors errors = new Errors();

    private Stage stage;

    private final Initializer initializer = new Initializer();
    private final BindingProcessor bindingProcesor;
    private final InjectionRequestProcessor injectionRequestProcessor;

    private final InjectorShell.Builder shellBuilder = new InjectorShell.Builder();
    private List<InjectorShell> shells;

    InjectorBuilder() {
        injectionRequestProcessor = new InjectionRequestProcessor(errors, initializer);
        bindingProcesor = new BindingProcessor(errors, initializer);
    }

    /**
     * Sets the stage for the created injector. If the stage is {@link Stage#PRODUCTION}, this class
     * will eagerly load singletons.
     */
    InjectorBuilder stage(Stage stage) {
        shellBuilder.stage(stage);
        this.stage = stage;
        return this;
    }

    /**
     * Sets the parent of the injector to-be-constructed. As a side effect, this sets this injector's
     * stage to the stage of {@code parent}.
     */
    InjectorBuilder parentInjector(InjectorImpl parent) {
        shellBuilder.parent(parent);
        return stage(parent.getInstance(Stage.class));
    }

    InjectorBuilder addModules(Iterable<? extends Module> modules) {
        shellBuilder.addModules(modules);
        return this;
    }

    Injector build() {
        if (shellBuilder == null) {
            throw new AssertionError("Already built, builders are not reusable.");
        }

        // Synchronize while we're building up the bindings and other injector state. This ensures that
        // the JIT bindings in the parent injector don't change while we're being built
        synchronized (shellBuilder.lock()) {
            shells = shellBuilder.build(initializer, bindingProcesor, stopwatch, errors);
            stopwatch.resetAndLog("Injector construction");

            initializeStatically();
        }

        // If we're in the tool stage, stop here. Don't eagerly inject or load anything.
        if (stage == Stage.TOOL) {
            return new ToolStageInjector(primaryInjector());
        }

        injectDynamically();

        return primaryInjector();
    }

    /**
     * Initialize and validate everything.
     */
    private void initializeStatically() {
        bindingProcesor.initializeBindings();
        stopwatch.resetAndLog("Binding initialization");

        for (InjectorShell shell : shells) {
            shell.getInjector().index();
        }
        stopwatch.resetAndLog("Binding indexing");

        injectionRequestProcessor.process(shells);
        stopwatch.resetAndLog("Collecting injection requests");

        bindingProcesor.runCreationListeners();
        stopwatch.resetAndLog("Binding validation");

        injectionRequestProcessor.validate();
        stopwatch.resetAndLog("Static validation");

        initializer.validateOustandingInjections(errors);
        stopwatch.resetAndLog("Instance member validation");

        new LookupProcessor(errors).process(shells);
        for (InjectorShell shell : shells) {
            ((DeferredLookups) shell.getInjector().lookups).initialize(errors);
        }
        stopwatch.resetAndLog("Provider verification");

        for (InjectorShell shell : shells) {
            if (!shell.getElements().isEmpty()) {
                throw new AssertionError("Failed to execute " + shell.getElements());
            }
        }

        errors.throwCreationExceptionIfErrorsExist();
    }

    /**
     * Returns the injector being constructed. This is not necessarily the root injector.
     */
    private Injector primaryInjector() {
        return shells.get(0).getInjector();
    }

    /**
     * Inject everything that can be injected. This method is intentionally not synchronized. If we
     * locked while injecting members (ie. running user code), things would deadlock should the user
     * code build a just-in-time binding from another thread.
     */
    private void injectDynamically() {
        injectionRequestProcessor.injectMembers();
        stopwatch.resetAndLog("Static member injection");

        initializer.injectAll(errors);
        stopwatch.resetAndLog("Instance injection");
        errors.throwCreationExceptionIfErrorsExist();

        for (InjectorShell shell : shells) {
            loadEagerSingletons(shell.getInjector(), stage, errors);
        }
        stopwatch.resetAndLog("Preloading singletons");
        errors.throwCreationExceptionIfErrorsExist();
    }

    /**
     * Loads eager singletons, or all singletons if we're in Stage.PRODUCTION. Bindings discovered
     * while we're binding these singletons are not be eager.
     */
    public void loadEagerSingletons(InjectorImpl injector, Stage stage, final Errors errors) {
        @SuppressWarnings("unchecked") // casting Collection<Binding> to Collection<BindingImpl> is safe
                Set<BindingImpl<?>> candidateBindings = ImmutableSet.copyOf(Iterables.concat(
                (Collection) injector.state.getExplicitBindingsThisLevel().values(),
                injector.jitBindings.values()));
        for (final BindingImpl<?> binding : candidateBindings) {
            if (binding.getScoping().isEagerSingleton(stage)) {
                try {
                    injector.callInContext(new ContextualCallable<Void>() {
                        Dependency<?> dependency = Dependency.get(binding.getKey());

                        @Override
                        public Void call(InternalContext context) {
                            context.setDependency(dependency);
                            Errors errorsForBinding = errors.withSource(dependency);
                            try {
                                binding.getInternalFactory().get(errorsForBinding, context, dependency);
                            } catch (ErrorsException e) {
                                errorsForBinding.merge(e.getErrors());
                            } finally {
                                context.setDependency(null);
                            }

                            return null;
                        }
                    });
                } catch (ErrorsException e) {
                    throw new AssertionError();
                }
            }
        }
    }

    /**
     * {@link Injector} exposed to users in {@link Stage#TOOL}.
     */
    static class ToolStageInjector implements Injector {
        private final Injector delegateInjector;

        ToolStageInjector(Injector delegateInjector) {
            this.delegateInjector = delegateInjector;
        }

        @Override
        public void injectMembers(Object o) {
            throw new UnsupportedOperationException(
                    "Injector.injectMembers(Object) is not supported in Stage.TOOL");
        }

        @Override
        public Map<Key<?>, Binding<?>> getBindings() {
            return this.delegateInjector.getBindings();
        }

        @Override
        public <T> Binding<T> getBinding(Key<T> key) {
            return this.delegateInjector.getBinding(key);
        }

        @Override
        public <T> Binding<T> getBinding(Class<T> type) {
            return this.delegateInjector.getBinding(type);
        }

        @Override
        public <T> List<Binding<T>> findBindingsByType(TypeLiteral<T> type) {
            return this.delegateInjector.findBindingsByType(type);
        }

        @Override
        public Injector getParent() {
            return delegateInjector.getParent();
        }

        @Override
        public Injector createChildInjector(Iterable<? extends Module> modules) {
            return delegateInjector.createChildInjector(modules);
        }

        @Override
        public Injector createChildInjector(Module... modules) {
            return delegateInjector.createChildInjector(modules);
        }

        @Override
        public <T> Provider<T> getProvider(Key<T> key) {
            throw new UnsupportedOperationException(
                    "Injector.getProvider(Key<T>) is not supported in Stage.TOOL");
        }

        @Override
        public <T> Provider<T> getProvider(Class<T> type) {
            throw new UnsupportedOperationException(
                    "Injector.getProvider(Class<T>) is not supported in Stage.TOOL");
        }

        @Override
        public <T> MembersInjector<T> getMembersInjector(TypeLiteral<T> typeLiteral) {
            throw new UnsupportedOperationException(
                    "Injector.getMembersInjector(TypeLiteral<T>) is not supported in Stage.TOOL");
        }

        @Override
        public <T> MembersInjector<T> getMembersInjector(Class<T> type) {
            throw new UnsupportedOperationException(
                    "Injector.getMembersInjector(Class<T>) is not supported in Stage.TOOL");
        }

        @Override
        public <T> T getInstance(Key<T> key) {
            throw new UnsupportedOperationException(
                    "Injector.getInstance(Key<T>) is not supported in Stage.TOOL");
        }

        @Override
        public <T> T getInstance(Class<T> type) {
            throw new UnsupportedOperationException(
                    "Injector.getInstance(Class<T>) is not supported in Stage.TOOL");
        }
    }
}
