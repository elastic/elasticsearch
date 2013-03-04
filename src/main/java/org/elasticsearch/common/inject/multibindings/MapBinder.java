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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.elasticsearch.common.inject.*;
import org.elasticsearch.common.inject.binder.LinkedBindingBuilder;
import org.elasticsearch.common.inject.multibindings.Multibinder.RealMultibinder;
import org.elasticsearch.common.inject.spi.Dependency;
import org.elasticsearch.common.inject.spi.ProviderWithDependencies;
import org.elasticsearch.common.inject.util.Types;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.elasticsearch.common.inject.multibindings.Multibinder.checkConfiguration;
import static org.elasticsearch.common.inject.multibindings.Multibinder.checkNotNull;
import static org.elasticsearch.common.inject.multibindings.Multibinder.setOf;
import static org.elasticsearch.common.inject.util.Types.newParameterizedTypeWithOwner;

/**
 * An API to bind multiple map entries separately, only to later inject them as
 * a complete map. MapBinder is intended for use in your application's module:
 * <pre><code>
 * public class SnacksModule extends AbstractModule {
 *   protected void configure() {
 *     MapBinder&lt;String, Snack&gt; mapbinder
 *         = MapBinder.newMapBinder(binder(), String.class, Snack.class);
 *     mapbinder.addBinding("twix").toInstance(new Twix());
 *     mapbinder.addBinding("snickers").toProvider(SnickersProvider.class);
 *     mapbinder.addBinding("skittles").to(Skittles.class);
 *   }
 * }</code></pre>
 *
 * <p>With this binding, a {@link Map}{@code <String, Snack>} can now be
 * injected:
 * <pre><code>
 * class SnackMachine {
 *   {@literal @}Inject
 *   public SnackMachine(Map&lt;String, Snack&gt; snacks) { ... }
 * }</code></pre>
 *
 * <p>In addition to binding {@code Map<K, V>}, a mapbinder will also bind
 * {@code Map<K, Provider<V>>} for lazy value provision:
 * <pre><code>
 * class SnackMachine {
 *   {@literal @}Inject
 *   public SnackMachine(Map&lt;String, Provider&lt;Snack&gt;&gt; snackProviders) { ... }
 * }</code></pre>
 *
 * <p>Contributing mapbindings from different modules is supported. For example,
 * it is okay to have both {@code CandyModule} and {@code ChipsModule} both
 * create their own {@code MapBinder<String, Snack>}, and to each contribute
 * bindings to the snacks map. When that map is injected, it will contain
 * entries from both modules.
 *
 * <p>The map's iteration order is consistent with the binding order. This is
 * convenient when multiple elements are contributed by the same module because
 * that module can order its bindings appropriately. Avoid relying on the
 * iteration order of elements contributed by different modules, since there is
 * no equivalent mechanism to order modules.
 *
 * <p>The map is unmodifiable.  Elements can only be added to the map by
 * configuring the MapBinder.  Elements can never be removed from the map.
 *
 * <p>Values are resolved at map injection time. If a value is bound to a
 * provider, that provider's get method will be called each time the map is
 * injected (unless the binding is also scoped, or a map of providers is injected).
 *
 * <p>Annotations are used to create different maps of the same key/value
 * type. Each distinct annotation gets its own independent map.
 *
 * <p><strong>Keys must be distinct.</strong> If the same key is bound more than
 * once, map injection will fail. However, use {@link #permitDuplicates()} in
 * order to allow duplicate keys; extra bindings to {@code Map<K, Set<V>>} and
 * {@code Map<K, Set<Provider<V>>} will be added.
 *
 * <p><strong>Keys must be non-null.</strong> {@code addBinding(null)} will
 * throw an unchecked exception.
 *
 * <p><strong>Values must be non-null to use map injection.</strong> If any
 * value is null, map injection will fail (although injecting a map of providers
 * will not).
 *
 * @author dpb@google.com (David P. Baker)
 */
public abstract class MapBinder<K, V> {
    private MapBinder() {}

    /**
     * Returns a new mapbinder that collects entries of {@code keyType}/{@code valueType} in a
     * {@link Map} that is itself bound with no binding annotation.
     */
    public static <K, V> MapBinder<K, V> newMapBinder(Binder binder,
                                                      TypeLiteral<K> keyType, TypeLiteral<V> valueType) {
        binder = binder.skipSources(MapBinder.class, RealMapBinder.class);
        return newMapBinder(binder, keyType, valueType,
                Key.get(mapOf(keyType, valueType)),
                Key.get(mapOfProviderOf(keyType, valueType)),
                Key.get(mapOf(keyType, setOf(valueType))),
                Key.get(mapOfSetOfProviderOf(keyType, valueType)),
                Multibinder.newSetBinder(binder, entryOfProviderOf(keyType, valueType)));
    }

    /**
     * Returns a new mapbinder that collects entries of {@code keyType}/{@code valueType} in a
     * {@link Map} that is itself bound with no binding annotation.
     */
    public static <K, V> MapBinder<K, V> newMapBinder(Binder binder,
                                                      Class<K> keyType, Class<V> valueType) {
        return newMapBinder(binder, TypeLiteral.get(keyType), TypeLiteral.get(valueType));
    }

    /**
     * Returns a new mapbinder that collects entries of {@code keyType}/{@code valueType} in a
     * {@link Map} that is itself bound with {@code annotation}.
     */
    public static <K, V> MapBinder<K, V> newMapBinder(Binder binder,
                                                      TypeLiteral<K> keyType, TypeLiteral<V> valueType, Annotation annotation) {
        binder = binder.skipSources(MapBinder.class, RealMapBinder.class);
        return newMapBinder(binder, keyType, valueType,
                Key.get(mapOf(keyType, valueType), annotation),
                Key.get(mapOfProviderOf(keyType, valueType), annotation),
                Key.get(mapOf(keyType, setOf(valueType)), annotation),
                Key.get(mapOfSetOfProviderOf(keyType, valueType), annotation),
                Multibinder.newSetBinder(binder, entryOfProviderOf(keyType, valueType), annotation));
    }

    /**
     * Returns a new mapbinder that collects entries of {@code keyType}/{@code valueType} in a
     * {@link Map} that is itself bound with {@code annotation}.
     */
    public static <K, V> MapBinder<K, V> newMapBinder(Binder binder,
                                                      Class<K> keyType, Class<V> valueType, Annotation annotation) {
        return newMapBinder(binder, TypeLiteral.get(keyType), TypeLiteral.get(valueType), annotation);
    }

    /**
     * Returns a new mapbinder that collects entries of {@code keyType}/{@code valueType} in a
     * {@link Map} that is itself bound with {@code annotationType}.
     */
    public static <K, V> MapBinder<K, V> newMapBinder(Binder binder, TypeLiteral<K> keyType,
                                                      TypeLiteral<V> valueType, Class<? extends Annotation> annotationType) {
        binder = binder.skipSources(MapBinder.class, RealMapBinder.class);
        return newMapBinder(binder, keyType, valueType,
                Key.get(mapOf(keyType, valueType), annotationType),
                Key.get(mapOfProviderOf(keyType, valueType), annotationType),
                Key.get(mapOf(keyType, setOf(valueType)), annotationType),
                Key.get(mapOfSetOfProviderOf(keyType, valueType), annotationType),
                Multibinder.newSetBinder(binder, entryOfProviderOf(keyType, valueType), annotationType));
    }

    /**
     * Returns a new mapbinder that collects entries of {@code keyType}/{@code valueType} in a
     * {@link Map} that is itself bound with {@code annotationType}.
     */
    public static <K, V> MapBinder<K, V> newMapBinder(Binder binder, Class<K> keyType,
                                                      Class<V> valueType, Class<? extends Annotation> annotationType) {
        return newMapBinder(
                binder, TypeLiteral.get(keyType), TypeLiteral.get(valueType), annotationType);
    }

    @SuppressWarnings("unchecked") // a map of <K, V> is safely a Map<K, V>
    static <K, V> TypeLiteral<Map<K, V>> mapOf(
            TypeLiteral<K> keyType, TypeLiteral<V> valueType) {
        return (TypeLiteral<Map<K, V>>) TypeLiteral.get(
                Types.mapOf(keyType.getType(), valueType.getType()));
    }

    @SuppressWarnings("unchecked") // a provider map <K, V> is safely a Map<K, Provider<V>>
    static <K, V> TypeLiteral<Map<K, Provider<V>>> mapOfProviderOf(
            TypeLiteral<K> keyType, TypeLiteral<V> valueType) {
        return (TypeLiteral<Map<K, Provider<V>>>) TypeLiteral.get(
                Types.mapOf(keyType.getType(), Types.providerOf(valueType.getType())));
    }

    @SuppressWarnings("unchecked") // a provider map <K, Set<V>> is safely a Map<K, Set<Provider<V>>>
    static <K, V> TypeLiteral<Map<K, Set<Provider<V>>>> mapOfSetOfProviderOf(
            TypeLiteral<K> keyType, TypeLiteral<V> valueType) {
        return (TypeLiteral<Map<K, Set<Provider<V>>>>) TypeLiteral.get(
                Types.mapOf(keyType.getType(), Types.setOf(Types.providerOf(valueType.getType()))));
    }

    @SuppressWarnings("unchecked") // a provider entry <K, V> is safely a Map.Entry<K, Provider<V>>
    static <K, V> TypeLiteral<Map.Entry<K, Provider<V>>> entryOfProviderOf(
            TypeLiteral<K> keyType, TypeLiteral<V> valueType) {
        return (TypeLiteral<Entry<K, Provider<V>>>) TypeLiteral.get(newParameterizedTypeWithOwner(
                Map.class, Entry.class, keyType.getType(), Types.providerOf(valueType.getType())));
    }

    private static <K, V> MapBinder<K, V> newMapBinder(Binder binder,
                                                       TypeLiteral<K> keyType, TypeLiteral<V> valueType,
                                                       Key<Map<K, V>> mapKey, Key<Map<K, Provider<V>>> providerMapKey,
                                                       Key<Map<K, Set<V>>> multimapKey, Key<Map<K, Set<Provider<V>>>> providerMultimapKey,
                                                       Multibinder<Entry<K, Provider<V>>> entrySetBinder) {
        RealMapBinder<K, V> mapBinder = new RealMapBinder<K, V>(
                binder, keyType, valueType, mapKey, providerMapKey, multimapKey,
                providerMultimapKey, entrySetBinder);
        binder.install(mapBinder);
        return mapBinder;
    }

    /**
     * Configures the {@code MapBinder} to handle duplicate entries.
     * <p>When multiple equal keys are bound, the value that gets included in the map is
     * arbitrary.
     * <p>In addition to the {@code Map<K, V>} and {@code Map<K, Provider<V>>}
     * maps that are normally bound, a {@code Map<K, Set<V>>} and
     * {@code Map<K, Set<Provider<V>>>} are <em>also</em> bound, which contain
     * all values bound to each key.
     * <p>
     * When multiple modules contribute elements to the map, this configuration
     * option impacts all of them.
     *
     * @return this map binder
     */
    public abstract MapBinder<K, V> permitDuplicates();

    /**
     * Returns a binding builder used to add a new entry in the map. Each
     * key must be distinct (and non-null). Bound providers will be evaluated each
     * time the map is injected.
     *
     * <p>It is an error to call this method without also calling one of the
     * {@code to} methods on the returned binding builder.
     *
     * <p>Scoping elements independently is supported. Use the {@code in} method
     * to specify a binding scope.
     */
    public abstract LinkedBindingBuilder<V> addBinding(K key);

    /**
     * The actual mapbinder plays several roles:
     *
     * <p>As a MapBinder, it acts as a factory for LinkedBindingBuilders for
     * each of the map's values. It delegates to a {@link Multibinder} of
     * entries (keys to value providers).
     *
     * <p>As a Module, it installs the binding to the map itself, as well as to
     * a corresponding map whose values are providers. It uses the entry set
     * multibinder to construct the map and the provider map.
     *
     * <p>As a module, this implements equals() and hashcode() in order to trick
     * Guice into executing its configure() method only once. That makes it so
     * that multiple mapbinders can be created for the same target map, but
     * only one is bound. Since the list of bindings is retrieved from the
     * injector itself (and not the mapbinder), each mapbinder has access to
     * all contributions from all equivalent mapbinders.
     *
     * <p>Rather than binding a single Map.Entry&lt;K, V&gt;, the map binder
     * binds keys and values independently. This allows the values to be properly
     * scoped.
     *
     * <p>We use a subclass to hide 'implements Module' from the public API.
     */
    private static final class RealMapBinder<K, V> extends MapBinder<K, V> implements Module {
        private final TypeLiteral<K> keyType;
        private final TypeLiteral<V> valueType;
        private final Key<Map<K, V>> mapKey;
        private final Key<Map<K, Provider<V>>> providerMapKey;
        private final Key<Map<K, Set<V>>> multimapKey;
        private final Key<Map<K, Set<Provider<V>>>> providerMultimapKey;
        private final RealMultibinder<Map.Entry<K, Provider<V>>> entrySetBinder;

        /* the target injector's binder. non-null until initialization, null afterwards */
        private Binder binder;

        private boolean permitDuplicates;
        private ImmutableList<Entry<K, Binding<V>>> mapBindings;

        private RealMapBinder(Binder binder, TypeLiteral<K> keyType, TypeLiteral<V> valueType,
                              Key<Map<K, V>> mapKey, Key<Map<K, Provider<V>>> providerMapKey,
                              Key<Map<K, Set<V>>> multimapKey, Key<Map<K, Set<Provider<V>>>> providerMultimapKey,
                              Multibinder<Map.Entry<K, Provider<V>>> entrySetBinder) {
            this.keyType = keyType;
            this.valueType = valueType;
            this.mapKey = mapKey;
            this.providerMapKey = providerMapKey;
            this.multimapKey = multimapKey;
            this.providerMultimapKey = providerMultimapKey;
            this.entrySetBinder = (RealMultibinder<Entry<K, Provider<V>>>) entrySetBinder;
            this.binder = binder;
        }

        @Override
        public MapBinder<K, V> permitDuplicates() {
            entrySetBinder.permitDuplicates();
            binder.install(new MultimapBinder<K, V>(
                    multimapKey, providerMultimapKey, entrySetBinder.getSetKey()));
            return this;
        }

        /**
         * This creates two bindings. One for the {@code Map.Entry<K, Provider<V>>}
         * and another for {@code V}.
         */
        @Override public LinkedBindingBuilder<V> addBinding(K key) {
            checkNotNull(key, "key");
            checkConfiguration(!isInitialized(), "MapBinder was already initialized");

            Key<V> valueKey = Key.get(valueType, new RealElement(entrySetBinder.getSetName()));
            entrySetBinder.addBinding().toInstance(new MapEntry<K, Provider<V>>(key,
                    binder.getProvider(valueKey), valueKey));
            return binder.bind(valueKey);
        }

        public void configure(Binder binder) {
            checkConfiguration(!isInitialized(), "MapBinder was already initialized");

            final ImmutableSet<Dependency<?>> dependencies
                    = ImmutableSet.<Dependency<?>>of(Dependency.get(entrySetBinder.getSetKey()));

            // Binds a Map<K, Provider<V>> from a collection of Set<Entry<K, Provider<V>>.
            final Provider<Set<Entry<K, Provider<V>>>> entrySetProvider = binder
                    .getProvider(entrySetBinder.getSetKey());
            binder.bind(providerMapKey).toProvider(new RealMapBinderProviderWithDependencies<Map<K, Provider<V>>>(mapKey) {
                private Map<K, Provider<V>> providerMap;

                @SuppressWarnings({ "unused", "unchecked" })
                @Inject void initialize(Injector injector) {
                    RealMapBinder.this.binder = null;
                    permitDuplicates = entrySetBinder.permitsDuplicates(injector);

                    Map<K, Provider<V>> providerMapMutable = new LinkedHashMap<K, Provider<V>>();
                    List<Entry<K, Binding<V>>> bindingsMutable = Lists.newArrayList();
                    for (Entry<K, Provider<V>> entry : entrySetProvider.get()) {
                        Provider<V> previous = providerMapMutable.put(entry.getKey(), entry.getValue());
                        checkConfiguration(previous == null || permitDuplicates,
                                "Map injection failed due to duplicated key \"%s\"", entry.getKey());
                        Key<V> valueKey = (Key<V>)((MapEntry)entry).getValueKey();
                        bindingsMutable.add(new MapEntry(entry.getKey(),
                                injector.getBinding(valueKey), valueKey));
                    }

                    providerMap = ImmutableMap.copyOf(providerMapMutable);
                    mapBindings = ImmutableList.copyOf(bindingsMutable);
                }

                public Map<K, Provider<V>> get() {
                    return providerMap;
                }

                public Set<Dependency<?>> getDependencies() {
                    return dependencies;
                }
            });

            final Provider<Map<K, Provider<V>>> mapProvider = binder.getProvider(providerMapKey);
            binder.bind(mapKey).toProvider(new RealMapWithExtensionProvider<Map<K, V>>(mapKey) {
                public Map<K, V> get() {
                    Map<K, V> map = new LinkedHashMap<K, V>();
                    for (Entry<K, Provider<V>> entry : mapProvider.get().entrySet()) {
                        V value = entry.getValue().get();
                        K key = entry.getKey();
                        checkConfiguration(value != null,
                                "Map injection failed due to null value for key \"%s\"", key);
                        map.put(key, value);
                    }
                    return Collections.unmodifiableMap(map);
                }

                public Set<Dependency<?>> getDependencies() {
                    return dependencies;
                }

                public Key<Map<K, V>> getMapKey() {
                    return mapKey;
                }

                public TypeLiteral<?> getKeyTypeLiteral() {
                    return keyType;
                }

                public TypeLiteral<?> getValueTypeLiteral() {
                    return valueType;
                }

                @SuppressWarnings("unchecked")
                public List<Entry<?, Binding<?>>> getEntries() {
                    if (isInitialized()) {
                        return (List)mapBindings; // safe because mapBindings is immutable
                    } else {
                        throw new UnsupportedOperationException("getElements() not supported for module bindings");
                    }
                }

                public boolean permitsDuplicates() {
                    if (isInitialized()) {
                        return permitDuplicates;
                    } else {
                        throw new UnsupportedOperationException("permitsDuplicates() not supported for module bindings");
                    }
                }

                public boolean containsElement(Element element) {
                    if (entrySetBinder.containsElement(element)) {
                        return true;
                    } else {
                        Key<?> key;
                        if (element instanceof Binding) {
                            key = ((Binding)element).getKey();
                        } else {
                            return false; // cannot match;
                        }

                        return key.equals(mapKey)
                                || key.equals(providerMapKey)
                                || key.equals(multimapKey)
                                || key.equals(providerMultimapKey)
                                || key.equals(entrySetBinder.getSetKey())
                                || matchesValueKey(key);
                    }
                }
            });
        }

        /** Returns true if the key indicates this is a value in the map. */
        private boolean matchesValueKey(Key key) {
            return key.getAnnotation() instanceof Element
                    && ((Element) key.getAnnotation()).setName().equals(entrySetBinder.getSetName())
                    && key.getTypeLiteral().equals(valueType);
        }

        private boolean isInitialized() {
            return binder == null;
        }

        @Override public boolean equals(Object o) {
            return o instanceof RealMapBinder
                    && ((RealMapBinder<?, ?>) o).mapKey.equals(mapKey);
        }

        @Override public int hashCode() {
            return mapKey.hashCode();
        }

        /**
         * Binds {@code Map<K, Set<V>>} and {{@code Map<K, Set<Provider<V>>>}.
         */
        private static final class MultimapBinder<K, V> implements Module {

            private final Key<Map<K, Set<V>>> multimapKey;
            private final Key<Map<K, Set<Provider<V>>>> providerMultimapKey;
            private final Key<Set<Entry<K,Provider<V>>>> entrySetKey;

            public MultimapBinder(
                    Key<Map<K, Set<V>>> multimapKey,
                    Key<Map<K, Set<Provider<V>>>> providerMultimapKey,
                    Key<Set<Entry<K,Provider<V>>>> entrySetKey) {
                this.multimapKey = multimapKey;
                this.providerMultimapKey = providerMultimapKey;
                this.entrySetKey = entrySetKey;
            }

            public void configure(Binder binder) {
                final ImmutableSet<Dependency<?>> dependencies
                        = ImmutableSet.<Dependency<?>>of(Dependency.get(entrySetKey));

                final Provider<Set<Entry<K, Provider<V>>>> entrySetProvider =
                        binder.getProvider(entrySetKey);
                // Binds a Map<K, Set<Provider<V>>> from a collection of Map<Entry<K, Provider<V>> if
                // permitDuplicates was called.
                binder.bind(providerMultimapKey).toProvider(
                        new RealMapBinderProviderWithDependencies<Map<K, Set<Provider<V>>>>(multimapKey) {
                            private Map<K, Set<Provider<V>>> providerMultimap;

                            @SuppressWarnings("unused")
                            @Inject void initialize(Injector injector) {
                                Map<K, ImmutableSet.Builder<Provider<V>>> providerMultimapMutable =
                                        new LinkedHashMap<K, ImmutableSet.Builder<Provider<V>>>();
                                for (Entry<K, Provider<V>> entry : entrySetProvider.get()) {
                                    if (!providerMultimapMutable.containsKey(entry.getKey())) {
                                        providerMultimapMutable.put(
                                                entry.getKey(), ImmutableSet.<Provider<V>>builder());
                                    }
                                    providerMultimapMutable.get(entry.getKey()).add(entry.getValue());
                                }

                                ImmutableMap.Builder<K, Set<Provider<V>>> providerMultimapBuilder =
                                        ImmutableMap.builder();
                                for (Entry<K, ImmutableSet.Builder<Provider<V>>> entry
                                        : providerMultimapMutable.entrySet()) {
                                    providerMultimapBuilder.put(entry.getKey(), entry.getValue().build());
                                }
                                providerMultimap = providerMultimapBuilder.build();
                            }

                            public Map<K, Set<Provider<V>>> get() {
                                return providerMultimap;
                            }

                            public Set<Dependency<?>> getDependencies() {
                                return dependencies;
                            }
                        });

                final Provider<Map<K, Set<Provider<V>>>> multimapProvider =
                        binder.getProvider(providerMultimapKey);
                binder.bind(multimapKey).toProvider(new RealMapBinderProviderWithDependencies<Map<K, Set<V>>>(multimapKey) {

                    public Map<K, Set<V>> get() {
                        ImmutableMap.Builder<K, Set<V>> multimapBuilder = ImmutableMap.builder();
                        for (Entry<K, Set<Provider<V>>> entry : multimapProvider.get().entrySet()) {
                            K key = entry.getKey();
                            ImmutableSet.Builder<V> valuesBuilder = ImmutableSet.builder();
                            for (Provider<V> valueProvider : entry.getValue()) {
                                V value = valueProvider.get();
                                checkConfiguration(value != null,
                                        "Multimap injection failed due to null value for key \"%s\"", key);
                                valuesBuilder.add(value);
                            }
                            multimapBuilder.put(key, valuesBuilder.build());
                        }
                        return multimapBuilder.build();
                    }

                    public Set<Dependency<?>> getDependencies() {
                        return dependencies;
                    }
                });
            }
        }

        private static final class MapEntry<K, V> implements Map.Entry<K, V> {
            private final K key;
            private final V value;
            private final Key<?> valueKey;

            private MapEntry(K key, V value, Key<?> valueKey) {
                this.key = key;
                this.value = value;
                this.valueKey = valueKey;
            }

            public Key<?> getValueKey() {
                return valueKey;
            }

            public K getKey() {
                return key;
            }

            public V getValue() {
                return value;
            }

            public V setValue(V value) {
                throw new UnsupportedOperationException();
            }

            @Override public boolean equals(Object obj) {
                return obj instanceof Map.Entry
                        && key.equals(((Map.Entry<?, ?>) obj).getKey())
                        && value.equals(((Map.Entry<?, ?>) obj).getValue());
            }

            @Override public int hashCode() {
                return 127 * ("key".hashCode() ^ key.hashCode())
                        + 127 * ("value".hashCode() ^ value.hashCode());
            }

            @Override public String toString() {
                return "MapEntry(" + key + ", " + value + ")";
            }
        }

        private static abstract class RealMapWithExtensionProvider<T>
                extends RealMapBinderProviderWithDependencies<T>
                implements Provider<T> {
            public RealMapWithExtensionProvider(Object equality) {
                super(equality);
            }
        }

        /**
         * A base class for ProviderWithDependencies that need equality
         * based on a specific object.
         */
        private static abstract class RealMapBinderProviderWithDependencies<T> implements ProviderWithDependencies<T> {
            private final Object equality;

            public RealMapBinderProviderWithDependencies(Object equality) {
                this.equality = equality;
            }

            @Override
            public boolean equals(Object obj) {
                return this.getClass() == obj.getClass() &&
                        equality.equals(((RealMapBinderProviderWithDependencies<?>)obj).equality);
            }

        }
    }
}
