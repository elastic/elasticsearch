/*
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

package org.elasticsearch.injection.guice;

import org.elasticsearch.injection.guice.internal.BindingImpl;
import org.elasticsearch.injection.guice.internal.Errors;
import org.elasticsearch.injection.guice.internal.ErrorsException;
import org.elasticsearch.injection.guice.internal.InternalContext;
import org.elasticsearch.injection.guice.internal.Scoping;
import org.elasticsearch.injection.guice.internal.Stopwatch;
import org.elasticsearch.injection.guice.spi.Dependency;

import java.util.List;

/**
 * Builds a tree of injectors. This is a primary injector. The primary injector is not necessarily a
 * top-level injector.
 * <p>
 * Injector construction happens in two phases.
 * <ol>
 * <li>Static building. In this phase, we interpret commands, create bindings, and inspect
 * dependencies. During this phase, we hold a lock to ensure consistency with parent injectors.
 * No user code is executed in this phase.</li>
 * <li>Dynamic injection. In this phase, we call user code. We inject members that requested
 * injection. This may require user's objects be created and their providers be called. And we
 * create eager singletons. In this phase, user code may have started other threads.
 * </ol>
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 */
class InjectorBuilder {

    private final Stopwatch stopwatch = new Stopwatch();
    private final Errors errors = new Errors();

    private final Initializer initializer = new Initializer();
    private final BindingProcessor bindingProcesor;

    private final InjectorShell.Builder shellBuilder = new InjectorShell.Builder();
    private List<InjectorShell> shells;

    InjectorBuilder() {
        bindingProcesor = new BindingProcessor(errors, initializer);
    }

    InjectorBuilder addModules(Iterable<? extends Module> modules) {
        shellBuilder.addModules(modules);
        return this;
    }

    Injector build() {

        // Synchronize while we're building up the bindings and other injector state. This ensures that
        // the JIT bindings in the parent injector don't change while we're being built
        synchronized (shellBuilder.lock()) {
            shells = shellBuilder.build(bindingProcesor, stopwatch, errors);
            stopwatch.resetAndLog("Injector construction");

            initializeStatically();
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

        stopwatch.resetAndLog("Collecting injection requests");

        bindingProcesor.runCreationListeners();
        stopwatch.resetAndLog("Binding validation");

        stopwatch.resetAndLog("Static validation");

        initializer.validateOustandingInjections(errors);
        stopwatch.resetAndLog("Instance member validation");

        new LookupProcessor(errors).process(shells);
        for (InjectorShell shell : shells) {
            ((DeferredLookups) shell.getInjector().lookups).initialize(errors);
        }
        stopwatch.resetAndLog("Provider verification");

        for (InjectorShell shell : shells) {
            if (shell.getElements().isEmpty() == false) {
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
        stopwatch.resetAndLog("Static member injection");

        initializer.injectAll(errors);
        stopwatch.resetAndLog("Instance injection");
        errors.throwCreationExceptionIfErrorsExist();

        for (InjectorShell shell : shells) {
            loadEagerSingletons(shell.getInjector(), errors);
        }
        stopwatch.resetAndLog("Preloading singletons");
        errors.throwCreationExceptionIfErrorsExist();
    }

    /**
     * Loads eager singletons. Bindings discovered while we're binding these singletons are not be eager.
     */
    public static void loadEagerSingletons(InjectorImpl injector, Errors errors) {
        for (final Binding<?> binding : injector.state.getExplicitBindingsThisLevel().values()) {
            loadEagerSingletons(injector, errors, (BindingImpl<?>) binding);
        }
        for (final BindingImpl<?> binding : injector.jitBindings.values()) {
            loadEagerSingletons(injector, errors, binding);
        }
    }

    private static void loadEagerSingletons(InjectorImpl injector, final Errors errors, BindingImpl<?> binding) {
        if (binding.getScoping() == Scoping.EAGER_SINGLETON) {
            try {
                injector.callInContext(new ContextualCallable<Void>() {
                    final Dependency<?> dependency = Dependency.get(binding.getKey());

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
