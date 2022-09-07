/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.plugin.scanner.impl;

import org.elasticsearch.plugin.scanner.NameToPluginInfo;
import org.elasticsearch.plugin.scanner.PluginInfo;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class StablePluginsRegistryImplTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testAddingNamedComponentsFromMultiplePlugins() {
        NamedComponentScanner scanner = Mockito.mock(NamedComponentScanner.class);
        ClassLoader loader = Mockito.mock(ClassLoader.class);
        ClassLoader loader2 = Mockito.mock(ClassLoader.class);

        NameToPluginInfo pluginInfo1 = new NameToPluginInfo().with(
            "namedComponentName1",
            new PluginInfo("namedComponentName1", "XXClassName", loader)
        );
        NameToPluginInfo pluginInfo2 = new NameToPluginInfo().with(
            "namedComponentName2",
            new PluginInfo("namedComponentName2", "YYClassName", loader)
        );
        NameToPluginInfo pluginInfo3 = new NameToPluginInfo().with(
            "namedComponentName3",
            new PluginInfo("namedComponentName3", "ZZClassName", loader2)
        );

        Mockito.when(scanner.findNamedComponents(ArgumentMatchers.any(Set.class), ArgumentMatchers.any(ClassLoader.class)))
            .thenReturn(Map.of("extensibleInterfaceName", pluginInfo1))
            .thenReturn(Map.of("extensibleInterfaceName", pluginInfo2))
            .thenReturn(Map.of("extensibleInterfaceName2", pluginInfo3));

        StablePluginsRegistryImpl registry = new StablePluginsRegistryImpl(scanner, new HashMap<>());
        registry.scanBundleForStablePlugins(Mockito.mock(Set.class), loader); // bundle 1
        registry.scanBundleForStablePlugins(Mockito.mock(Set.class), loader); // bundle 2
        registry.scanBundleForStablePlugins(Mockito.mock(Set.class), loader2); // bundle 3

        assertThat(
            registry.getNamedComponents(),
            Matchers.equalTo(
                Map.of(
                    "extensibleInterfaceName",
                    new NameToPluginInfo().with("namedComponentName1", new PluginInfo("namedComponentName1", "XXClassName", loader))
                        .with("namedComponentName2", new PluginInfo("namedComponentName2", "YYClassName", loader)),
                    "extensibleInterfaceName2",
                    new NameToPluginInfo().with("namedComponentName3", new PluginInfo("namedComponentName3", "ZZClassName", loader2))
                )
            )
        );
    }
}
