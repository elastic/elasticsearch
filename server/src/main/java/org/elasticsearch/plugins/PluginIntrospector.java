/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.core.SuppressForbidden;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

final class PluginIntrospector {

    private final Set<Class<?>> pluginClasses = Set.of(
        Plugin.class,
        ActionPlugin.class,
        AnalysisPlugin.class,
        CircuitBreakerPlugin.class,
        ClusterPlugin.class,
        DiscoveryPlugin.class,
        EnginePlugin.class,
        ExtensiblePlugin.class,
        HealthPlugin.class,
        IndexStorePlugin.class,
        IngestPlugin.class,
        MapperPlugin.class,
        NetworkPlugin.class,
        PersistentTaskPlugin.class,
        RecoveryPlannerPlugin.class,
        ReloadablePlugin.class,
        RepositoryPlugin.class,
        ScriptPlugin.class,
        SearchPlugin.class,
        ShutdownAwarePlugin.class,
        SystemIndexPlugin.class
    );

    private final Set<Class<?>> deprecatedPluginClasses = pluginClasses.stream()
        .filter(c -> c.isAnnotationPresent(Deprecated.class))
        .collect(Collectors.toUnmodifiableSet());

    private record MethodType(String name, Class<?>[] parameterTypes, boolean isDeprecated) {}

    private final Map<Class<?>, List<MethodType>> pluginMethodsMap;
    private final Map<Class<?>, List<MethodType>> pluginDeprecatedMethodsMap;

    private PluginIntrospector() {
        pluginMethodsMap = pluginClasses.stream().collect(toMap(Function.identity(), PluginIntrospector::findMethods));
        pluginDeprecatedMethodsMap = pluginMethodsMap.entrySet()
            .stream()
            .map(e -> Map.entry(e.getKey(), e.getValue().stream().filter(MethodType::isDeprecated).toList()))
            .filter(e -> e.getValue().isEmpty() == false)
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    static PluginIntrospector getInstance() {
        return new PluginIntrospector();
    }

    /**
     * Returns the list of Elasticsearch plugin interfaces implemented by the given plugin
     * implementation class. The list contains the simple names of the interfaces.
     */
    List<String> interfaces(final Class<?> pluginClass) {
        assert Plugin.class.isAssignableFrom(pluginClass);
        return interfaceClasses(pluginClass, pluginClasses::contains).map(Class::getSimpleName).sorted().toList();
    }

    /**
     * Returns the list of methods overridden by the given plugin implementation class. The list
     * contains the simple names of the methods.
     */
    List<String> overriddenMethods(final Class<?> pluginClass) {
        return findOverriddenMethods(pluginClass, pluginMethodsMap).keySet().stream().sorted().toList();
    }

    /**
     * Returns the list of deprecated Elasticsearch plugin interfaces which are implemented by the
     * given plugin implementation class. The list contains the simple names of the interfaces.
     */
    List<String> deprecatedInterfaces(final Class<?> pluginClass) {
        assert Plugin.class.isAssignableFrom(pluginClass);
        return interfaceClasses(pluginClass, deprecatedPluginClasses::contains).map(Class::getSimpleName).sorted().toList();
    }

    /**
     * Returns the deprecated methods from Elasticsearch plugin interfaces which are implemented by
     * the given plugin implementation class. The map is from the simple method name to the simple interface class name.
     *
     * @apiNote The simple names work as a key because they are unique across all plugin interfaces.
     */
    Map<String, String> deprecatedMethods(final Class<?> pluginClass) {
        return findOverriddenMethods(pluginClass, pluginDeprecatedMethodsMap);
    }

    // finds the subset of given methods that are overridden by the given class
    // returns a map of method name to interface name the method was declared in
    private static Map<String, String> findOverriddenMethods(final Class<?> pluginClass, Map<Class<?>, List<MethodType>> methodsMap) {
        assert Plugin.class.isAssignableFrom(pluginClass);
        List<Class<?>> clazzes = Stream.concat(Stream.of(Plugin.class), interfaceClasses(pluginClass, methodsMap::containsKey)).toList();
        if (clazzes.isEmpty()) {
            return Map.of();
        }

        Map<String, String> overriddenMethods = new HashMap<>();
        for (var clazz : clazzes) {
            List<MethodType> methods = methodsMap.get(clazz);
            if (methods == null) {
                continue;
            }
            for (var mt : methods) {
                try {
                    Method m = pluginClass.getMethod(mt.name(), mt.parameterTypes());
                    if (m.getDeclaringClass() == clazz) {
                        // it's not overridden
                    } else {
                        assert clazz.isAssignableFrom(m.getDeclaringClass());
                        var existing = overriddenMethods.put(mt.name(), clazz.getSimpleName());
                        assert existing == null;
                    }
                } catch (NoSuchMethodException unexpected) {
                    throw new AssertionError(unexpected);
                }
            }
        }
        return Map.copyOf(overriddenMethods);
    }

    // Returns the non-static methods declared in the given class.
    @SuppressForbidden(reason = "Need declared methods")
    private static List<MethodType> findMethods(Class<?> cls) {
        assert cls.getName().startsWith("org.elasticsearch.plugins");
        assert cls.isInterface() || cls == Plugin.class : cls;
        return Arrays.stream(cls.getDeclaredMethods())
            .filter(m -> Modifier.isStatic(m.getModifiers()) == false)
            .map(m -> new MethodType(m.getName(), m.getParameterTypes(), m.isAnnotationPresent(Deprecated.class)))
            .toList();
    }

    // Returns a stream of o.e.XXXPlugin interfaces, that the given plugin class implements.
    private static Stream<Class<?>> interfaceClasses(Class<?> pluginClass, Predicate<Class<?>> classPredicate) {
        assert Plugin.class.isAssignableFrom(pluginClass);
        Set<Class<?>> pluginInterfaces = new HashSet<>();
        do {
            Arrays.stream(pluginClass.getInterfaces()).forEach(inf -> superInterfaces(inf, pluginInterfaces, classPredicate));
        } while ((pluginClass = pluginClass.getSuperclass()) != java.lang.Object.class);
        return pluginInterfaces.stream();
    }

    private static void superInterfaces(Class<?> c, Set<Class<?>> interfaces, Predicate<Class<?>> classPredicate) {
        if (classPredicate.test(c)) {
            interfaces.add(c);
        }
        Arrays.stream(c.getInterfaces()).forEach(inf -> superInterfaces(inf, interfaces, classPredicate));
    }
}
