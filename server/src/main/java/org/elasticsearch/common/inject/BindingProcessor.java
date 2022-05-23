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

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.internal.Annotations;
import org.elasticsearch.common.inject.internal.BindingImpl;
import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.internal.ErrorsException;
import org.elasticsearch.common.inject.internal.InstanceBindingImpl;
import org.elasticsearch.common.inject.internal.InternalFactory;
import org.elasticsearch.common.inject.internal.LinkedBindingImpl;
import org.elasticsearch.common.inject.internal.LinkedProviderBindingImpl;
import org.elasticsearch.common.inject.internal.ProviderInstanceBindingImpl;
import org.elasticsearch.common.inject.internal.ProviderMethod;
import org.elasticsearch.common.inject.internal.Scoping;
import org.elasticsearch.common.inject.internal.UntargettedBindingImpl;
import org.elasticsearch.common.inject.spi.BindingTargetVisitor;
import org.elasticsearch.common.inject.spi.InjectionPoint;
import org.elasticsearch.common.inject.spi.InstanceBinding;
import org.elasticsearch.common.inject.spi.LinkedKeyBinding;
import org.elasticsearch.common.inject.spi.ProviderInstanceBinding;
import org.elasticsearch.common.inject.spi.ProviderKeyBinding;
import org.elasticsearch.common.inject.spi.UntargettedBinding;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Handles {@link Binder#bind} elements.
 *
 * @author crazybob@google.com (Bob Lee)
 * @author jessewilson@google.com (Jesse Wilson)
 */
class BindingProcessor extends AbstractProcessor {

    private final List<CreationListener> creationListeners = new ArrayList<>();
    private final Initializer initializer;
    private final List<Runnable> uninitializedBindings = new ArrayList<>();

    BindingProcessor(Errors errors, Initializer initializer) {
        super(errors);
        this.initializer = initializer;
    }

    @Override
    public <T> Boolean visit(Binding<T> command) {
        final Object source = command.getSource();

        if (Void.class.equals(command.getKey().getRawType())) {
            if (command instanceof ProviderInstanceBinding
                && ((ProviderInstanceBinding<?>) command).getProviderInstance() instanceof ProviderMethod) {
                errors.voidProviderMethod();
            } else {
                errors.missingConstantValues();
            }
            return true;
        }

        final Key<T> key = command.getKey();
        Class<? super T> rawType = key.getTypeLiteral().getRawType();

        if (rawType == Provider.class) {
            errors.bindingToProvider();
            return true;
        }

        validateKey(command.getSource(), command.getKey());

        final Scoping scoping = Scopes.makeInjectable(((BindingImpl<?>) command).getScoping(), injector, errors);

        command.acceptTargetVisitor(new BindingTargetVisitor<T, Void>() {

            @Override
            public Void visit(InstanceBinding<? extends T> binding) {
                Set<InjectionPoint> injectionPoints = binding.getInjectionPoints();
                T instance = binding.getInstance();
                Initializable<T> ref = initializer.requestInjection(injector, instance, source, injectionPoints);
                ConstantFactory<? extends T> factory = new ConstantFactory<>(ref);
                InternalFactory<? extends T> scopedFactory = Scopes.scope(injector, factory, scoping);
                putBinding(new InstanceBindingImpl<>(injector, key, source, scopedFactory, injectionPoints, instance));
                return null;
            }

            @Override
            public Void visit(ProviderInstanceBinding<? extends T> binding) {
                Provider<? extends T> provider = binding.getProviderInstance();
                Set<InjectionPoint> injectionPoints = binding.getInjectionPoints();
                Initializable<Provider<? extends T>> initializable = initializer.requestInjection(
                    injector,
                    provider,
                    source,
                    injectionPoints
                );
                InternalFactory<T> factory = new InternalFactoryToProviderAdapter<>(initializable, source);
                InternalFactory<? extends T> scopedFactory = Scopes.scope(injector, factory, scoping);
                putBinding(new ProviderInstanceBindingImpl<>(injector, key, source, scopedFactory, scoping, provider, injectionPoints));
                return null;
            }

            @Override
            public Void visit(ProviderKeyBinding<? extends T> binding) {
                Key<? extends Provider<? extends T>> providerKey = binding.getProviderKey();
                BoundProviderFactory<T> boundProviderFactory = new BoundProviderFactory<>(injector, providerKey, source);
                creationListeners.add(boundProviderFactory);
                InternalFactory<? extends T> scopedFactory = Scopes.scope(injector, boundProviderFactory, scoping);
                putBinding(new LinkedProviderBindingImpl<>(injector, key, source, scopedFactory, scoping, providerKey));
                return null;
            }

            @Override
            public Void visit(LinkedKeyBinding<? extends T> binding) {
                Key<? extends T> linkedKey = binding.getLinkedKey();
                if (key.equals(linkedKey)) {
                    errors.recursiveBinding();
                }

                FactoryProxy<T> factory = new FactoryProxy<>(injector, key, linkedKey, source);
                creationListeners.add(factory);
                InternalFactory<? extends T> scopedFactory = Scopes.scope(injector, factory, scoping);
                putBinding(new LinkedBindingImpl<>(injector, key, source, scopedFactory, scoping, linkedKey));
                return null;
            }

            @Override
            public Void visit(UntargettedBinding<? extends T> untargetted) {
                // Error: Missing implementation.
                // Example: bind(Date.class).annotatedWith(Red.class);
                // We can't assume abstract types aren't injectable. They may have an
                // @ImplementedBy annotation or something.
                if (key.hasAnnotationType()) {
                    errors.missingImplementation(key);
                    putBinding(invalidBinding(injector, key, source));
                    return null;
                }

                // This cast is safe after the preceding check.
                final BindingImpl<T> binding;
                try {
                    binding = injector.createUnitializedBinding(key, scoping, source, errors);
                    putBinding(binding);
                } catch (ErrorsException e) {
                    errors.merge(e.getErrors());
                    putBinding(invalidBinding(injector, key, source));
                    return null;
                }

                uninitializedBindings.add(() -> {
                    try {
                        ((InjectorImpl) binding.getInjector()).initializeBinding(binding, errors.withSource(source));
                    } catch (ErrorsException e) {
                        errors.merge(e.getErrors());
                    }
                });

                return null;
            }

            @Override
            public Void visit() {
                throw new IllegalArgumentException("Cannot apply a non-module element");
            }
        });

        return true;
    }

    private <T> void validateKey(Object source, Key<T> key) {
        Annotations.checkForMisplacedScopeAnnotations(key.getRawType(), source, errors);
    }

    static <T> UntargettedBindingImpl<T> invalidBinding(InjectorImpl injector, Key<T> key, Object source) {
        return new UntargettedBindingImpl<>(injector, key, source);
    }

    public void initializeBindings() {
        for (Runnable initializer : uninitializedBindings) {
            initializer.run();
        }
    }

    public void runCreationListeners() {
        for (CreationListener creationListener : creationListeners) {
            creationListener.notify(errors);
        }
    }

    private void putBinding(BindingImpl<?> binding) {
        Key<?> key = binding.getKey();

        Class<?> rawType = key.getRawType();
        if (FORBIDDEN_TYPES.contains(rawType)) {
            errors.cannotBindToGuiceType(rawType.getSimpleName());
            return;
        }

        Binding<?> original = injector.state.getExplicitBinding(key);
        if (original != null) {
            errors.bindingAlreadySet(key, original.getSource());
            return;
        }

        // prevent the parent from creating a JIT binding for this key
        injector.state.parent().blacklist(key);
        injector.state.putBinding(key, binding);
    }

    // It's unfortunate that we have to maintain a blacklist of specific
    // classes, but we can't easily block the whole package because of
    // all our unit tests.
    private static final Set<Class<?>> FORBIDDEN_TYPES = Set.of(
        AbstractModule.class,
        Binder.class,
        Binding.class,
        Injector.class,
        Key.class,
        MembersInjector.class,
        Module.class,
        Provider.class,
        Scope.class,
        TypeLiteral.class
    );
    // TODO(jessewilson): fix BuiltInModule, then add Stage

    interface CreationListener {
        void notify(Errors errors);
    }
}
