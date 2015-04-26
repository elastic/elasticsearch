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

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.inject.*;
import org.elasticsearch.common.inject.binder.LinkedBindingBuilder;
import org.elasticsearch.common.inject.multibindings.Multibinder.RealMultibinder;
import org.elasticsearch.common.inject.spi.Dependency;
import org.elasticsearch.common.inject.spi.ProviderWithDependencies;
import org.elasticsearch.common.inject.util.Types;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.elasticsearch.common.inject.util.Types.newParameterizedType;
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
 * <p/>
 * <p>With this binding, a {@link Map}{@code <String, Snack>} can now be
 * injected:
 * <pre><code>
 * class SnackMachine {
 *   {@literal @}Inject
 *   public SnackMachine(Map&lt;String, Snack&gt; snacks) { ... }
 * }</code></pre>
 * <p/>
 * <p>In addition to binding {@code Map<K, V>}, a mapbinder will also bind
 * {@code Map<K, Provider<V>>} for lazy value provision:
 * <pre><code>
 * class SnackMachine {
 *   {@literal @}Inject
 *   public SnackMachine(Map&lt;String, Provider&lt;Snack&gt;&gt; snackProviders) { ... }
 * }</code></pre>
 * <p/>
 * <p>Creating mapbindings from different modules is supported. For example, it
 * is okay to have both {@code CandyModule} and {@code ChipsModule} both
 * create their own {@code MapBinder<String, Snack>}, and to each contribute
 * bindings to the snacks map. When that map is injected, it will contain
 * entries from both modules.
 * <p/>
 * <p>Values are resolved at map injection time. If a value is bound to a
 * provider, that provider's get method will be called each time the map is
 * injected (unless the binding is also scoped, or a map of providers is injected).
 * <p/>
 * <p>Annotations are used to create different maps of the same key/value
 * type. Each distinct annotation gets its own independent map.
 * <p/>
 * <p><strong>Keys must be distinct.</strong> If the same key is bound more than
 * once, map injection will fail.
 * <p/>
 * <p><strong>Keys must be non-null.</strong> {@code addBinding(null)} will
 * throw an unchecked exception.
 * <p/>
 * <p><strong>Values must be non-null to use map injection.</strong> If any
 * value is null, map injection will fail (although injecting a map of providers
 * will not).
 *
 * @author dpb@google.com (David P. Baker)
 */
public abstract class MapBinder<K, V> {
    private MapBinder() {
    }

    /**
     * Returns a new mapbinder that collects entries of {@code keyType}/{@code valueType} in a
     * {@link Map} that is itself bound with no binding annotation.
     */
    public static <K, V> MapBinder<K, V> newMapBinder(Binder binder,
                                                      TypeLiteral<K> keyType, TypeLiteral<V> valueType) {
        binder = binder.skipSources(MapBinder.class, RealMapBinder.class);
        return newMapBinder(binder, valueType,
                Key.get(mapOf(keyType, valueType)),
                Key.get(mapOfProviderOf(keyType, valueType)),
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
        return newMapBinder(binder, valueType,
                Key.get(mapOf(keyType, valueType), annotation),
                Key.get(mapOfProviderOf(keyType, valueType), annotation),
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
        return newMapBinder(binder, valueType,
                Key.get(mapOf(keyType, valueType), annotationType),
                Key.get(mapOfProviderOf(keyType, valueType), annotationType),
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
    private static <K, V> TypeLiteral<Map<K, V>> mapOf(
            TypeLiteral<K> keyType, TypeLiteral<V> valueType) {
        return (TypeLiteral<Map<K, V>>) TypeLiteral.get(
                Types.mapOf(keyType.getType(), valueType.getType()));
    }

    @SuppressWarnings("unchecked") // a provider map <K, V> is safely a Map<K, Provider<V>>
    private static <K, V> TypeLiteral<Map<K, Provider<V>>> mapOfProviderOf(
            TypeLiteral<K> keyType, TypeLiteral<V> valueType) {
        return (TypeLiteral<Map<K, Provider<V>>>) TypeLiteral.get(
                Types.mapOf(keyType.getType(), newParameterizedType(Provider.class, valueType.getType())));
    }

    @SuppressWarnings("unchecked") // a provider entry <K, V> is safely a Map.Entry<K, Provider<V>>
    private static <K, V> TypeLiteral<Map.Entry<K, Provider<V>>> entryOfProviderOf(
            TypeLiteral<K> keyType, TypeLiteral<V> valueType) {
        return (TypeLiteral<Entry<K, Provider<V>>>) TypeLiteral.get(newParameterizedTypeWithOwner(
                Map.class, Entry.class, keyType.getType(), Types.providerOf(valueType.getType())));
    }

    private static <K, V> MapBinder<K, V> newMapBinder(Binder binder, TypeLiteral<V> valueType,
                                                       Key<Map<K, V>> mapKey, Key<Map<K, Provider<V>>> providerMapKey,
                                                       Multibinder<Entry<K, Provider<V>>> entrySetBinder) {
        RealMapBinder<K, V> mapBinder = new RealMapBinder<>(
                binder, valueType, mapKey, providerMapKey, entrySetBinder);
        binder.install(mapBinder);
        return mapBinder;
    }

    /**
     * Returns a binding builder used to add a new entry in the map. Each
     * key must be distinct (and non-null). Bound providers will be evaluated each
     * time the map is injected.
     * <p/>
     * <p>It is an error to call this method without also calling one of the
     * {@code to} methods on the returned binding builder.
     * <p/>
     * <p>Scoping elements independently is supported. Use the {@code in} method
     * to specify a binding scope.
     */
    public abstract LinkedBindingBuilder<V> addBinding(K key);

    /**
     * The actual mapbinder plays several roles:
     * <p/>
     * <p>As a MapBinder, it acts as a factory for LinkedBindingBuilders for
     * each of the map's values. It delegates to a {@link Multibinder} of
     * entries (keys to value providers).
     * <p/>
     * <p>As a Module, it installs the binding to the map itself, as well as to
     * a corresponding map whose values are providers. It uses the entry set
     * multibinder to construct the map and the provider map.
     * <p/>
     * <p>As a module, this implements equals() and hashcode() in order to trick
     * Guice into executing its configure() method only once. That makes it so
     * that multiple mapbinders can be created for the same target map, but
     * only one is bound. Since the list of bindings is retrieved from the
     * injector itself (and not the mapbinder), each mapbinder has access to
     * all contributions from all equivalent mapbinders.
     * <p/>
     * <p>Rather than binding a single Map.Entry&lt;K, V&gt;, the map binder
     * binds keys and values independently. This allows the values to be properly
     * scoped.
     * <p/>
     * <p>We use a subclass to hide 'implements Module' from the public API.
     */
    private static final class RealMapBinder<K, V> extends MapBinder<K, V> implements Module {
        private final TypeLiteral<V> valueType;
        private final Key<Map<K, V>> mapKey;
        private final Key<Map<K, Provider<V>>> providerMapKey;
        private final RealMultibinder<Map.Entry<K, Provider<V>>> entrySetBinder;

        /* the target injector's binder. non-null until initialization, null afterwards */
        private Binder binder;

        private RealMapBinder(Binder binder, TypeLiteral<V> valueType,
                              Key<Map<K, V>> mapKey, Key<Map<K, Provider<V>>> providerMapKey,
                              Multibinder<Map.Entry<K, Provider<V>>> entrySetBinder) {
            this.valueType = valueType;
            this.mapKey = mapKey;
            this.providerMapKey = providerMapKey;
            this.entrySetBinder = (RealMultibinder<Entry<K, Provider<V>>>) entrySetBinder;
            this.binder = binder;
        }

        /**
         * This creates two bindings. One for the {@code Map.Entry<K, Provider<V>>}
         * and another for {@code V}.
         */
        @Override
        public LinkedBindingBuilder<V> addBinding(K key) {
            Multibinder.checkNotNull(key, "key");
            Multibinder.checkConfiguration(!isInitialized(), "MapBinder was already initialized");

            Key<V> valueKey = Key.get(valueType, new RealElement(entrySetBinder.getSetName()));
            entrySetBinder.addBinding().toInstance(new MapEntry<>(key,
                    binder.getProvider(valueKey)));
            return binder.bind(valueKey);
        }

        @Override
        public void configure(Binder binder) {
            Multibinder.checkConfiguration(!isInitialized(), "MapBinder was already initialized");

            final ImmutableSet<Dependency<?>> dependencies
                    = ImmutableSet.<Dependency<?>>of(Dependency.get(entrySetBinder.getSetKey()));

            // binds a Map<K, Provider<V>> from a collection of Map<Entry<K, Provider<V>>
            final Provider<Set<Entry<K, Provider<V>>>> entrySetProvider = binder
                    .getProvider(entrySetBinder.getSetKey());
            binder.bind(providerMapKey).toProvider(new ProviderWithDependencies<Map<K, Provider<V>>>() {
                private Map<K, Provider<V>> providerMap;

                @SuppressWarnings("unused")
                @Inject
                void initialize() {
                    RealMapBinder.this.binder = null;

                    Map<K, Provider<V>> providerMapMutable = new LinkedHashMap<>();
                    for (Entry<K, Provider<V>> entry : entrySetProvider.get()) {
                        Multibinder.checkConfiguration(providerMapMutable.put(entry.getKey(), entry.getValue()) == null,
                                "Map injection failed due to duplicated key \"%s\"", entry.getKey());
                    }

                    providerMap = Collections.unmodifiableMap(providerMapMutable);
                }

                @Override
                public Map<K, Provider<V>> get() {
                    return providerMap;
                }

                @Override
                public Set<Dependency<?>> getDependencies() {
                    return dependencies;
                }
            });

            final Provider<Map<K, Provider<V>>> mapProvider = binder.getProvider(providerMapKey);
            binder.bind(mapKey).toProvider(new ProviderWithDependencies<Map<K, V>>() {
                @Override
                public Map<K, V> get() {
                    Map<K, V> map = new LinkedHashMap<>();
                    for (Entry<K, Provider<V>> entry : mapProvider.get().entrySet()) {
                        V value = entry.getValue().get();
                        K key = entry.getKey();
                        Multibinder.checkConfiguration(value != null,
                                "Map injection failed due to null value for key \"%s\"", key);
                        map.put(key, value);
                    }
                    return Collections.unmodifiableMap(map);
                }

                @Override
                public Set<Dependency<?>> getDependencies() {
                    return dependencies;
                }
            });
        }

        private boolean isInitialized() {
            return binder == null;
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof RealMapBinder
                    && ((RealMapBinder<?, ?>) o).mapKey.equals(mapKey);
        }

        @Override
        public int hashCode() {
            return mapKey.hashCode();
        }

        private static final class MapEntry<K, V> implements Map.Entry<K, V> {
            private final K key;
            private final V value;

            private MapEntry(K key, V value) {
                this.key = key;
                this.value = value;
            }

            @Override
            public K getKey() {
                return key;
            }

            @Override
            public V getValue() {
                return value;
            }

            @Override
            public V setValue(V value) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean equals(Object obj) {
                return obj instanceof Map.Entry
                        && key.equals(((Map.Entry<?, ?>) obj).getKey())
                        && value.equals(((Map.Entry<?, ?>) obj).getValue());
            }

            @Override
            public int hashCode() {
                return 127 * ("key".hashCode() ^ key.hashCode())
                        + 127 * ("value".hashCode() ^ value.hashCode());
            }

            @Override
            public String toString() {
                return "MapEntry(" + key + ", " + value + ")";
            }
        }
    }
}
