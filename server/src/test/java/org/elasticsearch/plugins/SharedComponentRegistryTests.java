/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;

public class SharedComponentRegistryTests extends ESTestCase {

    private static class MyComponent {}

    private interface MyService {}

    private static class MyServiceImpl implements MyService {}

    public void testGet() {
        var registry = new SharedComponentRegistry();

        expectThrows(IllegalStateException.class, "MyComponent is not registered yet", () -> registry.get(MyComponent.class));

        var instance = new MyComponent();
        registry.register(MyComponent.class, instance);
        assertThat(registry.get(MyComponent.class), sameInstance(instance));
    }

    public void testGetWithImplementation() {
        var registry = new SharedComponentRegistry();
        var instance = new MyServiceImpl();
        registry.register(MyService.class, instance);

        var result = registry.get(MyService.class);
        assertThat(result, sameInstance(instance));
        assertThat(result, instanceOf(MyServiceImpl.class));
    }

    public void testMultipleRegisterFail() {
        var registry = new SharedComponentRegistry();
        registry.register(MyComponent.class, new MyComponent());

        expectThrows(
            IllegalStateException.class,
            "MyComponent is already registered",
            () -> registry.register(MyComponent.class, new MyComponent())
        );
    }

    public void testGetProvider() {
        var registry = new SharedComponentRegistry();

        // obtain a provider before the component is registered — no exception
        var provider = registry.getProvider(MyComponent.class);

        // calling the provider before registration fails
        expectThrows(IllegalStateException.class, "MyComponent is not registered yet", provider::get);

        // after registration, the provider resolves to the registered instance
        var instance = new MyComponent();
        registry.register(MyComponent.class, instance);
        assertThat(provider.get(), sameInstance(instance));
    }
}
