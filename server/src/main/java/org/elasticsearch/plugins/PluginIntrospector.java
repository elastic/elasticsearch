/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.plugins.interceptor.RestInterceptorActionPlugin;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

final class PluginIntrospector {

    // TODO: assert that this list is complete and correct
    private final Set<Class<?>> pluginClasses = Set.of(
        Plugin.class,
        ActionPlugin.class,
        AnalysisPlugin.class,
        CircuitBreakerPlugin.class,
        ClusterPlugin.class,
        DiscoveryPlugin.class,
        EnginePlugin.class,
        ExtensiblePlugin.class, // TODO: not sure that we want this?
        HealthPlugin.class,
        IndexStorePlugin.class,
        IngestPlugin.class,
        RestInterceptorActionPlugin.class,
        MapperPlugin.class,
        // MetadataUpgrader.class, // TODO: not sure that we want this?
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

    private final Map<Class<?>, List<MethodType>> pluginMethodsMap;

    private PluginIntrospector() {
        pluginMethodsMap = pluginClasses.stream().collect(toMap(Function.identity(), PluginIntrospector::findMethods));
    }

    static PluginIntrospector getInstance() {
        return new PluginIntrospector();
    }

    /**
     * Returns the list of Elasticsearch plugin interfaces implemented by the given plugin
     * implementation class.
     */
    List<String> interfaces(Class<?> pluginClass) {
        assert Plugin.class.isAssignableFrom(pluginClass);
        return interfaceClasses(pluginClass).map(Class::getName).sorted().toList();
    }

    /**
     * Returns the list of methods overridden by the given plugin implementation class.
     *
     * For example, if a plugin implementation class overrides the createComponents method, this
     * method would return a list containing the string entry:
     *    org.elasticsearch.plugins.Plugin.createComponents(
     *      org.elasticsearch.client.internal.Client,org.elasticsearch.cluster.service.ClusterService,
     *      org.elasticsearch.threadpool.ThreadPool,org.elasticsearch.watcher.ResourceWatcherService,
     *      org.elasticsearch.script.ScriptService,org.elasticsearch.xcontent.NamedXContentRegistry,
     *      org.elasticsearch.env.Environment,org.elasticsearch.env.NodeEnvironment,
     *      org.elasticsearch.common.io.stream.NamedWriteableRegistry,
     *      org.elasticsearch.cluster.metadata.IndexNameExpressionResolver,
     *      java.util.function.Supplier)
     */
    List<String> overriddenMethods(Class<?> pluginClass) {
        assert Plugin.class.isAssignableFrom(pluginClass);
        List<Class<?>> implClasses = Stream.concat(Stream.of(Plugin.class), interfaceClasses(pluginClass)).toList();

        List<String> overriddenMethods = new ArrayList<>();
        for (var implClass : implClasses) {
            List<MethodType> pluginMethods = pluginMethodsMap.get(implClass);
            // assert pluginMethods != null : "no plugin methods for " + implClass;
            // log ^^^
            for (var mt : pluginMethods) {
                try {
                    Method m = pluginClass.getMethod(mt.name(), mt.parameterTypes());
                    if (m.getDeclaringClass() == implClass) {
                        // it's not overridden
                    } else {
                        assert implClass.isAssignableFrom(m.getDeclaringClass());
                        overriddenMethods.add(methodToString(implClass, mt));
                    }
                } catch (NoSuchMethodException unexpected) {
                    throw new AssertionError(unexpected);
                }
            }
        }
        return List.copyOf(overriddenMethods);
    }

    private record MethodType(String name, Class<?>[] parameterTypes) {}

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

    // Returns a String representation for the given method type in the given class.
    private static String methodToString(Class<?> cls, MethodType mt) {
        StringBuilder sb = new StringBuilder();
        sb.append(cls.getName()).append(".");
        sb.append(mt.name());
        sb.append(Arrays.stream(mt.parameterTypes()).map(Type::getTypeName).collect(Collectors.joining(",", "(", ")")));
        return sb.toString();
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
