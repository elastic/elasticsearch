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

package org.elasticsearch.common.inject.util;

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Binding;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.PrivateBinder;
import org.elasticsearch.common.inject.Scope;
import org.elasticsearch.common.inject.spi.DefaultBindingScopingVisitor;
import org.elasticsearch.common.inject.spi.DefaultElementVisitor;
import org.elasticsearch.common.inject.spi.Element;
import org.elasticsearch.common.inject.spi.Elements;
import org.elasticsearch.common.inject.spi.PrivateElements;
import org.elasticsearch.common.inject.spi.ScopeBinding;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.util.set.Sets.newHashSet;

/**
 * Static utility methods for creating and working with instances of {@link Module}.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 * @since 2.0
 */
public final class Modules {
    private Modules() {
    }

    public static final Module EMPTY_MODULE = new Module() {
        @Override
        public void configure(Binder binder) {
        }
    };

    /**
     * Returns a builder that creates a module that overlays override modules over the given
     * modules. If a key is bound in both sets of modules, only the binding from the override modules
     * is kept. This can be used to replace the bindings of a production module with test bindings:
     * <pre>
     * Module functionalTestModule
     *     = Modules.override(new ProductionModule()).with(new TestModule());
     * </pre>
     * <p>
     * Prefer to write smaller modules that can be reused and tested without overrides.
     *
     * @param modules the modules whose bindings are open to be overridden
     */
    public static OverriddenModuleBuilder override(Module... modules) {
        return new RealOverriddenModuleBuilder(Arrays.asList(modules));
    }

    /**
     * Returns a builder that creates a module that overlays override modules over the given
     * modules. If a key is bound in both sets of modules, only the binding from the override modules
     * is kept. This can be used to replace the bindings of a production module with test bindings:
     * <pre>
     * Module functionalTestModule
     *     = Modules.override(getProductionModules()).with(getTestModules());
     * </pre>
     * <p>
     * Prefer to write smaller modules that can be reused and tested without overrides.
     *
     * @param modules the modules whose bindings are open to be overridden
     */
    public static OverriddenModuleBuilder override(Iterable<? extends Module> modules) {
        return new RealOverriddenModuleBuilder(modules);
    }

    /**
     * Returns a new module that installs all of {@code modules}.
     */
    public static Module combine(Module... modules) {
        return combine(Arrays.asList(modules));
    }

    /**
     * Returns a new module that installs all of {@code modules}.
     */
    public static Module combine(Iterable<? extends Module> modules) {
        final Set<? extends Module> modulesSet = newHashSet(modules);
        return new Module() {
            @Override
            public void configure(Binder binder) {
                binder = binder.skipSources(getClass());
                for (Module module : modulesSet) {
                    binder.install(module);
                }
            }
        };
    }

    /**
     * See the EDSL example at {@link Modules#override(Module[]) override()}.
     */
    public interface OverriddenModuleBuilder {

        /**
         * See the EDSL example at {@link Modules#override(Module[]) override()}.
         */
        Module with(Module... overrides);

        /**
         * See the EDSL example at {@link Modules#override(Module[]) override()}.
         */
        Module with(Iterable<? extends Module> overrides);
    }

    private static final class RealOverriddenModuleBuilder implements OverriddenModuleBuilder {
        private final Set<Module> baseModules;

        private RealOverriddenModuleBuilder(Iterable<? extends Module> baseModules) {
            HashSet<? extends Module> modules = newHashSet(baseModules);
            this.baseModules = unmodifiableSet(modules);
        }

        @Override
        public Module with(Module... overrides) {
            return with(Arrays.asList(overrides));
        }

        @Override
        public Module with(final Iterable<? extends Module> overrides) {
            return new AbstractModule() {
                @Override
                public void configure() {
                    final List<Element> elements = Elements.getElements(baseModules);
                    final List<Element> overrideElements = Elements.getElements(overrides);

                    final Set<Key> overriddenKeys = new HashSet<>();
                    final Set<Class<? extends Annotation>> overridesScopeAnnotations = new HashSet<>();

                    // execute the overrides module, keeping track of which keys and scopes are bound
                    new ModuleWriter(binder()) {
                        @Override
                        public <T> Void visit(Binding<T> binding) {
                            overriddenKeys.add(binding.getKey());
                            return super.visit(binding);
                        }

                        @Override
                        public Void visit(ScopeBinding scopeBinding) {
                            overridesScopeAnnotations.add(scopeBinding.getAnnotationType());
                            return super.visit(scopeBinding);
                        }

                        @Override
                        public Void visit(PrivateElements privateElements) {
                            overriddenKeys.addAll(privateElements.getExposedKeys());
                            return super.visit(privateElements);
                        }
                    }.writeAll(overrideElements);

                    // execute the original module, skipping all scopes and overridden keys. We only skip each
                    // overridden binding once so things still blow up if the module binds the same thing
                    // multiple times.
                    final Map<Scope, Object> scopeInstancesInUse = new HashMap<>();
                    final List<ScopeBinding> scopeBindings = new ArrayList<>();
                    new ModuleWriter(binder()) {
                        @Override
                        public <T> Void visit(Binding<T> binding) {
                            if (!overriddenKeys.remove(binding.getKey())) {
                                super.visit(binding);

                                // Record when a scope instance is used in a binding
                                Scope scope = getScopeInstanceOrNull(binding);
                                if (scope != null) {
                                    scopeInstancesInUse.put(scope, binding.getSource());
                                }
                            }

                            return null;
                        }

                        @Override
                        public Void visit(PrivateElements privateElements) {
                            PrivateBinder privateBinder = binder.withSource(privateElements.getSource())
                                    .newPrivateBinder();

                            Set<Key<?>> skippedExposes = new HashSet<>();

                            for (Key<?> key : privateElements.getExposedKeys()) {
                                if (overriddenKeys.remove(key)) {
                                    skippedExposes.add(key);
                                } else {
                                    privateBinder.withSource(privateElements.getExposedSource(key)).expose(key);
                                }
                            }

                            // we're not skipping deep exposes, but that should be okay. If we ever need to, we
                            // have to search through this set of elements for PrivateElements, recursively
                            for (Element element : privateElements.getElements()) {
                                if (element instanceof Binding
                                        && skippedExposes.contains(((Binding) element).getKey())) {
                                    continue;
                                }
                                element.applyTo(privateBinder);
                            }

                            return null;
                        }

                        @Override
                        public Void visit(ScopeBinding scopeBinding) {
                            scopeBindings.add(scopeBinding);
                            return null;
                        }
                    }.writeAll(elements);

                    // execute the scope bindings, skipping scopes that have been overridden. Any scope that
                    // is overridden and in active use will prompt an error
                    new ModuleWriter(binder()) {
                        @Override
                        public Void visit(ScopeBinding scopeBinding) {
                            if (!overridesScopeAnnotations.remove(scopeBinding.getAnnotationType())) {
                                super.visit(scopeBinding);
                            } else {
                                Object source = scopeInstancesInUse.get(scopeBinding.getScope());
                                if (source != null) {
                                    binder().withSource(source).addError(
                                            "The scope for @%s is bound directly and cannot be overridden.",
                                            scopeBinding.getAnnotationType().getSimpleName());
                                }
                            }
                            return null;
                        }
                    }.writeAll(scopeBindings);

                    // TODO: bind the overridden keys using multibinder
                }

                private Scope getScopeInstanceOrNull(Binding<?> binding) {
                    return binding.acceptScopingVisitor(new DefaultBindingScopingVisitor<Scope>() {
                        @Override
                        public Scope visitScope(Scope scope) {
                            return scope;
                        }
                    });
                }
            };
        }
    }

    private static class ModuleWriter extends DefaultElementVisitor<Void> {
        protected final Binder binder;

        ModuleWriter(Binder binder) {
            this.binder = binder;
        }

        @Override
        protected Void visitOther(Element element) {
            element.applyTo(binder);
            return null;
        }

        void writeAll(Iterable<? extends Element> elements) {
            for (Element element : elements) {
                element.acceptVisitor(this);
            }
        }
    }
}
