/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.elasticsearch.plugins.PluginBundle;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;

public class StablePluginsRegistryTests extends ESTestCase {

    public void testAddingNamedComponentsFromMultiplePlugins() {
        NamedComponentReader scanner = Mockito.mock(NamedComponentReader.class);
        ClassLoader loader = Mockito.mock(ClassLoader.class);
        ClassLoader loader2 = Mockito.mock(ClassLoader.class);

        NameToPluginInfo pluginInfo1 = new NameToPluginInfo().put(
            "namedComponentName1",
            new PluginInfo("namedComponentName1", "XXClassName", loader)
        );
        NameToPluginInfo pluginInfo2 = new NameToPluginInfo().put(
            "namedComponentName2",
            new PluginInfo("namedComponentName2", "YYClassName", loader)
        );
        NameToPluginInfo pluginInfo3 = new NameToPluginInfo().put(
            "namedComponentName3",
            new PluginInfo("namedComponentName3", "ZZClassName", loader2)
        );

        Mockito.when(scanner.findNamedComponents(any(PluginBundle.class), any(ClassLoader.class)))
            .thenReturn(Map.of("extensibleInterfaceName", pluginInfo1))
            .thenReturn(Map.of("extensibleInterfaceName", pluginInfo2))
            .thenReturn(Map.of("extensibleInterfaceName2", pluginInfo3));

        StablePluginsRegistry registry = new StablePluginsRegistry(scanner, new HashMap<>());
        registry.scanBundleForStablePlugins(Mockito.mock(PluginBundle.class), loader); // bundle 1
        registry.scanBundleForStablePlugins(Mockito.mock(PluginBundle.class), loader); // bundle 2
        registry.scanBundleForStablePlugins(Mockito.mock(PluginBundle.class), loader2); // bundle 3

        assertThat(
            registry.getNamedComponents(),
            Matchers.equalTo(
                Map.of(
                    "extensibleInterfaceName",
                    new NameToPluginInfo().put("namedComponentName1", new PluginInfo("namedComponentName1", "XXClassName", loader))
                        .put("namedComponentName2", new PluginInfo("namedComponentName2", "YYClassName", loader)),
                    "extensibleInterfaceName2",
                    new NameToPluginInfo().put("namedComponentName3", new PluginInfo("namedComponentName3", "ZZClassName", loader2))
                )
            )
        );
    }
}
