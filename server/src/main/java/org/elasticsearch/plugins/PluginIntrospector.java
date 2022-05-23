/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final class PluginIntrospector {

    /** The reverse DNS prefix of the Elasticsearch code base. */
    static final String ES_NAME_PREFIX = "org.elasticsearch.";

    /**
     * Returns the list of Elasticsearch interfaces (and superinterfaces) implemented by the given plugin.
     */
    static List<String> interfaces(Class<?> pluginClass) {
        assert Plugin.class.isAssignableFrom(pluginClass);

        List<String> interfaces = new ArrayList<>();
        do {
            Arrays.stream(pluginClass.getInterfaces()).forEach(inf -> superInterfaces(inf, interfaces));
        } while ((pluginClass = pluginClass.getSuperclass()) != java.lang.Object.class);
        return interfaces.stream().sorted().toList();
    }

    private static void superInterfaces(Class<?> c, List<String> interfaces) {
        if (isESClass(c)) {
            interfaces.add(c.getName());
        }
        Arrays.stream(c.getInterfaces()).forEach(inf -> superInterfaces(inf, interfaces));
    }

    private static boolean isESClass(Class<?> c) {
        return c.getName().startsWith(ES_NAME_PREFIX);
    }
}
