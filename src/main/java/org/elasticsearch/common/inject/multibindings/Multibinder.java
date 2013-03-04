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

package org.elasticsearch.common.inject.multibindings;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Binding;
import org.elasticsearch.common.inject.ConfigurationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Key;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.TypeLiteral;
import org.elasticsearch.common.inject.binder.LinkedBindingBuilder;
import org.elasticsearch.common.inject.internal.Annotations;
import org.elasticsearch.common.inject.internal.Errors;

import org.elasticsearch.common.inject.spi.BindingTargetVisitor;
import org.elasticsearch.common.inject.spi.Dependency;
import org.elasticsearch.common.inject.spi.HasDependencies;
import org.elasticsearch.common.inject.spi.Message;
import org.elasticsearch.common.inject.spi.ProviderInstanceBinding;
import org.elasticsearch.common.inject.util.Types;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.inject.name.Names.named;

/**
 * An API to bind multiple values separately, only to later inject them as a
 * complete collection. Multibinder is intended for use in your application's
 * module:
 * <pre><code>
 * public class SnacksModule extends AbstractModule {
 *   protected void configure() {
 *     Multibinder&lt;Snack&gt; multibinder
 *         = Multibinder.newSetBinder(binder(), Snack.class);
 *     multibinder.addBinding().toInstance(new Twix());
 *     multibinder.addBinding().toProvider(SnickersProvider.class);
 *     multibinder.addBinding().to(Skittles.class);
 *   }
 * }</code></pre>
 *
 * <p>With this binding, a {@link Set}{@code <Snack>} can now be injected:
 * <pre><code>
 * class SnackMachine {
 *   {@literal @}Inject
 *   public SnackMachine(Set&lt;Snack&gt; snacks) { ... }
 * }</code></pre>
 *
 * <p>Contributing multibindings from different modules is supported. For
 * example, it is okay to have both {@code CandyModule} and {@code ChipsModule}
 * to both create their own {@code Multibinder<Snack>}, and to each contribute
 * bindings to the set of snacks. When that set is injected, it will contain
 * elements from both modules.
 *
 * <p>The set's iteration order is consistent with the binding order. This is
 * convenient when multiple elements are contributed by the same module because
 * that module can order its bindings appropriately. Avoid relying on the
 * iteration order of elements contributed by different modules, since there is
 * no equivalent mechanism to order modules.
 *
 * <p>The set is unmodifiable.  Elements can only be added to the set by
 * configuring the multibinder.  Elements can never be removed from the set.
 *
 * <p>Elements are resolved at set injection time. If an element is bound to a
 * provider, that provider's get method will be called each time the set is
 * injected (unless the binding is also scoped).
 *
 * <p>Annotations are be used to create different sets of the same element
 * type. Each distinct annotation gets its own independent collection of
 * elements.
 *
 * <p><strong>Elements must be distinct.</strong> If multiple bound elements
 * have the same value, set injection will fail.
 *
 * <p><strong>Elements must be non-null.</strong> If any set element is null,
 * set injection will fail.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
public abstract class Multibinder<T> {
    private Multibinder() {}

    /**
     * Returns a new multibinder that collects instances of {@code type} in a {@link Set} that is
     * itself bound with no binding annotation.
     */
    public static <T> Multibinder<T> newSetBinder(Binder binder, TypeLiteral<T> type) {
        binder = binder.skipSources(RealMultibinder.class, Multibinder.class);
        RealMultibinder<T> result = new RealMultibinder<T>(binder, type,
                Key.get(Multibinder.<T>setOf(type)));
        binder.install(result);
        return result;
    }

    /**
     * Returns a new multibinder that collects instances of {@code type} in a {@link Set} that is
     * itself bound with no binding annotation.
     */
    public static <T> Multibinder<T> newSetBinder(Binder binder, Class<T> type) {
        return newSetBinder(binder, TypeLiteral.get(type));
    }

    /**
     * Returns a new multibinder that collects instances of {@code type} in a {@link Set} that is
     * itself bound with {@code annotation}.
     */
    public static <T> Multibinder<T> newSetBinder(
            Binder binder, TypeLiteral<T> type, Annotation annotation) {
        binder = binder.skipSources(RealMultibinder.class, Multibinder.class);
        RealMultibinder<T> result = new RealMultibinder<T>(binder, type,
                Key.get(Multibinder.<T>setOf(type), annotation));
        binder.install(result);
        return result;
    }

    /**
     * Returns a new multibinder that collects instances of {@code type} in a {@link Set} that is
     * itself bound with {@code annotation}.
     */
    public static <T> Multibinder<T> newSetBinder(
            Binder binder, Class<T> type, Annotation annotation) {
        return newSetBinder(binder, TypeLiteral.get(type), annotation);
    }

    /**
     * Returns a new multibinder that collects instances of {@code type} in a {@link Set} that is
     * itself bound with {@code annotationType}.
     */
    public static <T> Multibinder<T> newSetBinder(Binder binder, TypeLiteral<T> type,
                                                  Class<? extends Annotation> annotationType) {
        binder = binder.skipSources(RealMultibinder.class, Multibinder.class);
        RealMultibinder<T> result = new RealMultibinder<T>(binder, type,
                Key.get(Multibinder.<T>setOf(type), annotationType));
        binder.install(result);
        return result;
    }

    /**
     * Returns a new multibinder that collects instances of {@code type} in a {@link Set} that is
     * itself bound with {@code annotationType}.
     */
    public static <T> Multibinder<T> newSetBinder(Binder binder, Class<T> type,
                                                  Class<? extends Annotation> annotationType) {
        return newSetBinder(binder, TypeLiteral.get(type), annotationType);
    }

    @SuppressWarnings("unchecked") // wrapping a T in a Set safely returns a Set<T>
    static <T> TypeLiteral<Set<T>> setOf(TypeLiteral<T> elementType) {
        Type type = Types.setOf(elementType.getType());
        return (TypeLiteral<Set<T>>) TypeLiteral.get(type);
    }

    /**
     * Configures the bound set to silently discard duplicate elements. When multiple equal values are
     * bound, the one that gets included is arbitrary. When multiple modules contribute elements to
     * the set, this configuration option impacts all of them.
     *
     * @return this multibinder
     */
    public abstract Multibinder<T> permitDuplicates();

    /**
     * Returns a binding builder used to add a new element in the set. Each
     * bound element must have a distinct value. Bound providers will be
     * evaluated each time the set is injected.
     *
     * <p>It is an error to call this method without also calling one of the
     * {@code to} methods on the returned binding builder.
     *
     * <p>Scoping elements independently is supported. Use the {@code in} method
     * to specify a binding scope.
     */
    public abstract LinkedBindingBuilder<T> addBinding();

    /**
     * The actual multibinder plays several roles:
     *
     * <p>As a Multibinder, it acts as a factory for LinkedBindingBuilders for
     * each of the set's elements. Each binding is given an annotation that
     * identifies it as a part of this set.
     *
     * <p>As a Module, it installs the binding to the set itself. As a module,
     * this implements equals() and hashcode() in order to trick Guice into
     * executing its configure() method only once. That makes it so that
     * multiple multibinders can be created for the same target collection, but
     * only one is bound. Since the list of bindings is retrieved from the
     * injector itself (and not the multibinder), each multibinder has access to
     * all contributions from all multibinders.
     *
     * <p>As a Provider, this constructs the set instances.
     *
     * <p>We use a subclass to hide 'implements Module, Provider' from the public
     * API.
     */
    static final class RealMultibinder<T> extends Multibinder<T>
            implements Module, HasDependencies, Provider<Set<T>> {

        private final TypeLiteral<T> elementType;
        private final String setName;
        private final Key<Set<T>> setKey;
        private final Key<Boolean> permitDuplicatesKey;

        /* the target injector's binder. non-null until initialization, null afterwards */
        private Binder binder;

        /* a binding for each element in the set. null until initialization, non-null afterwards */
        private ImmutableList<Binding<T>> bindings;
        private Set<Dependency<?>> dependencies;

        /** whether duplicates are allowed. Possibly configured by a different instance */
        private boolean permitDuplicates;

        private RealMultibinder(Binder binder, TypeLiteral<T> elementType, Key<Set<T>> setKey) {
            this.binder = checkNotNull(binder, "binder");
            this.elementType = checkNotNull(elementType, "elementType");
            this.setKey = checkNotNull(setKey, "setKey");
            this.setName = nameOf(setKey);
            this.permitDuplicatesKey = Key.get(Boolean.class, named(toString() + " permits duplicates"));
        }

        /**
         * Returns the name the set should use.  This is based on the annotation.
         * If the annotation has an instance and is not a marker annotation,
         * we ask the annotation for its toString.  If it was a marker annotation
         * or just an annotation type, we use the annotation's name. Otherwise,
         * the name is the empty string.
         */
        private String nameOf(Key<?> key) {
            Annotation annotation = setKey.getAnnotation();
            Class<? extends Annotation> annotationType = setKey.getAnnotationType();
            if (annotation != null && !Annotations.isMarker(annotationType)) {
                return setKey.getAnnotation().toString();
            } else if(setKey.getAnnotationType() != null) {
                return "@" + setKey.getAnnotationType().getName();
            } else {
                return "";
            }
        }

        @SuppressWarnings("unchecked")
        public void configure(Binder binder) {
            checkConfiguration(!isInitialized(), "Multibinder was already initialized");
            binder.bind(setKey).toProvider(this);
        }

        @Override
        public Multibinder<T> permitDuplicates() {
            binder.install(new PermitDuplicatesModule(permitDuplicatesKey));
            return this;
        }

        @Override public LinkedBindingBuilder<T> addBinding() {
            checkConfiguration(!isInitialized(), "Multibinder was already initialized");

            return binder.bind(Key.get(elementType, new RealElement(setName)));
        }

        /**
         * Invoked by Guice at Injector-creation time to prepare providers for each
         * element in this set. At this time the set's size is known, but its
         * contents are only evaluated when get() is invoked.
         */
        @Inject void initialize(Injector injector) {
            List<Binding<T>> bindings = Lists.newArrayList();
            List<Dependency<?>> dependencies = Lists.newArrayList();
            for (Binding<?> entry : injector.findBindingsByType(elementType)) {
                if (keyMatches(entry.getKey())) {
                    @SuppressWarnings("unchecked") // protected by findBindingsByType()
                    Binding<T> binding = (Binding<T>) entry;
                    bindings.add(binding);
                    dependencies.add(Dependency.get(binding.getKey()));
                }
            }

            this.bindings = ImmutableList.copyOf(bindings);
            this.dependencies = ImmutableSet.copyOf(dependencies);
            this.permitDuplicates = permitsDuplicates(injector);
            this.binder = null;
        }

        boolean permitsDuplicates(Injector injector) {
            return injector.getBindings().containsKey(permitDuplicatesKey);
        }

        private boolean keyMatches(Key<?> key) {
            return key.getTypeLiteral().equals(elementType)
                    && key.getAnnotation() instanceof Element
                    && ((Element) key.getAnnotation()).setName().equals(setName);
        }

        private boolean isInitialized() {
            return binder == null;
        }

        public Set<T> get() {
            checkConfiguration(isInitialized(), "Multibinder is not initialized");

            Set<T> result = new LinkedHashSet<T>();
            for (Binding<T> binding : bindings) {
                final T newValue = binding.getProvider().get();
                checkConfiguration(newValue != null, "Set injection failed due to null element");
                checkConfiguration(result.add(newValue) || permitDuplicates,
                        "Set injection failed due to duplicated element \"%s\"", newValue);
            }
            return Collections.unmodifiableSet(result);
        }

        String getSetName() {
            return setName;
        }

        public Key<Set<T>> getSetKey() {
            return setKey;
        }

        @SuppressWarnings("unchecked")
        public List<Binding<?>> getElements() {
            if(isInitialized()) {
                return (List)bindings; // safe because bindings is immutable.
            } else {
                throw new UnsupportedOperationException("getElements() not supported for module bindings");
            }
        }

        public boolean containsElement(Element element) {
            if(element instanceof Binding) {
                Binding binding = (Binding)element;
                return keyMatches(binding.getKey())
                        || binding.getKey().equals(permitDuplicatesKey)
                        || binding.getKey().equals(setKey);
            } else {
                return false;
            }
        }

        public Set<Dependency<?>> getDependencies() {
            if (!isInitialized()) {
                return ImmutableSet.<Dependency<?>>of(Dependency.get(Key.get(Injector.class)));
            } else {
                return dependencies;
            }
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof RealMultibinder
                    && ((RealMultibinder<?>) o).setKey.equals(setKey);
        }

        @Override
        public int hashCode() {
            return setKey.hashCode();
        }

        @Override
        public String toString() {
            return new StringBuilder()
                    .append(setName)
                    .append(setName.length() > 0 ? " " : "")
                    .append("Multibinder<")
                    .append(elementType)
                    .append(">")
                    .toString();
        }
    }

    /**
     * We install the permit duplicates configuration as its own binding, all by itself. This way,
     * if only one of a multibinder's users remember to call permitDuplicates(), they're still
     * permitted.
     */
    private static class PermitDuplicatesModule extends AbstractModule {
        private final Key<Boolean> key;

        PermitDuplicatesModule(Key<Boolean> key) {
            this.key = key;
        }

        @Override
        protected void configure() {
            bind(key).toInstance(true);
        }

        @Override public boolean equals(Object o) {
            return o instanceof PermitDuplicatesModule
                    && ((PermitDuplicatesModule) o).key.equals(key);
        }

        @Override public int hashCode() {
            return getClass().hashCode() ^ key.hashCode();
        }
    }

    static void checkConfiguration(boolean condition, String format, Object... args) {
        if (condition) {
            return;
        }

        throw new ConfigurationException(ImmutableSet.of(new Message(Errors.format(format, args))));
    }

    static <T> T checkNotNull(T reference, String name) {
        if (reference != null) {
            return reference;
        }

        NullPointerException npe = new NullPointerException(name);
        throw new ConfigurationException(ImmutableSet.of(
                new Message(ImmutableList.of(), npe.toString(), npe)));
    }
}