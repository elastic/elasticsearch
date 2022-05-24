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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
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

    private record MethodType(String name, Class<?>[] parameterTypes) {}

    private final Map<Class<?>, List<MethodType>> pluginMethodsMap;

    private PluginIntrospector() {
        pluginMethodsMap = pluginClasses.stream().collect(toMap(Function.identity(), PluginIntrospector::findMethods));
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
        return interfaceClasses(pluginClass).map(Class::getSimpleName).sorted().toList();
    }

    /**
     * Returns the list of methods overridden by the given plugin implementation class. The list
     * contains the simple names of the methods.
     */
    List<String> overriddenMethods(final Class<?> pluginClass) {
        assert Plugin.class.isAssignableFrom(pluginClass);
        List<Class<?>> esPluginClasses = Stream.concat(Stream.of(Plugin.class), interfaceClasses(pluginClass)).toList();

        List<String> overriddenMethods = new ArrayList<>();
        for (var esPluginClass : esPluginClasses) {
            List<MethodType> esPluginMethods = pluginMethodsMap.get(esPluginClass);
            assert esPluginMethods != null : "no plugin methods for " + esPluginClass;
            for (var mt : esPluginMethods) {
                try {
                    Method m = pluginClass.getMethod(mt.name(), mt.parameterTypes());
                    if (m.getDeclaringClass() == esPluginClass) {
                        // it's not overridden
                    } else {
                        assert esPluginClass.isAssignableFrom(m.getDeclaringClass());
                        overriddenMethods.add(mt.name());
                    }
                } catch (NoSuchMethodException unexpected) {
                    throw new AssertionError(unexpected);
                }
            }
        }
        return overriddenMethods.stream().sorted().toList();
    }

    // Returns the non-static methods declared in the given class.
    @SuppressForbidden(reason = "Need declared methods")
    private static List<MethodType> findMethods(Class<?> cls) {
        assert cls.getName().startsWith("org.elasticsearch.plugins");
        assert cls.isInterface() || cls == Plugin.class : cls;
        return Arrays.stream(cls.getDeclaredMethods())
            .filter(m -> Modifier.isStatic(m.getModifiers()) == false)
            .map(m -> new MethodType(m.getName(), m.getParameterTypes()))
            .toList();
    }

    // Returns a stream of o.e.XXXPlugin interfaces, that the given plugin class implements.
    private Stream<Class<?>> interfaceClasses(Class<?> pluginClass) {
        assert Plugin.class.isAssignableFrom(pluginClass);
        Set<Class<?>> pluginInterfaces = new HashSet<>();
        do {
            Arrays.stream(pluginClass.getInterfaces()).forEach(inf -> superInterfaces(inf, pluginInterfaces));
        } while ((pluginClass = pluginClass.getSuperclass()) != java.lang.Object.class);
        return pluginInterfaces.stream();
    }

    private void superInterfaces(Class<?> c, Set<Class<?>> interfaces) {
        if (isESPlugin(c)) {
            interfaces.add(c);
        }
        Arrays.stream(c.getInterfaces()).forEach(inf -> superInterfaces(inf, interfaces));
    }

    private boolean isESPlugin(Class<?> c) {
        return pluginClasses.contains(c);
    }
}
