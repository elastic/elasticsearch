/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.node;

import org.elasticsearch.plugins.ExtensiblePlugin;
import org.elasticsearch.plugins.Plugin;

/**
 *<p>
 * Describes the relationship between an interface and an implementation for a {@link Plugin} component. All {@link Plugin}'s
 * provide a collection of components via the createComponents(...) that are made available for
 * dependency injection. An {@link ExtensiblePlugin} may want to expose an interface such that extensions can change the implementation.
 * Usage of this class allows the interface to made available for dependency injection where the implementation is defined at runtime.
 *</p>
 *     Usage:
 * <pre>{@code
public class MyPlugin extends Plugin implements ExtensiblePlugin
{
    @Override
    public Collection<Object> createComponents(...) {
        List<Object> components = new ArrayList<>();
        components.add(new PluginComponentBinding<>(MyInterface.class, this.myImplementation));
        ...
        return components;
    }
    @Override
    public void loadExtensions(ExtensionLoader loader) {
        List<MyInterface> myImplementations = loader.loadExtensions(MyInterface.class);
        if(myImplementations.size() > 0) {
            this.myImplementation = myImplementations.get(0); //use extension provided implementation
        } else {
            this.myImplementation = new StandardMyImplementation()
        }
    }
...
}
public class TransportMyAction extends TransportMasterNodeAction<MyRequest, MyResponse> {
    @Inject
    public TransportMyAction(MyInterface myInterface) {
        this.myInterface = myInterface; //implementation may vary depending on extensions defined for
    }
    ...
}
}</pre>
 *<p>
 * Note - usage of the class does not change any class loader isolation strategies nor visibility of the provided classes.
 * </p>
 * @param <I> The interface class
 * @param <T> The implementation class
 */
public record PluginComponentBinding<I, T extends I>(Class<? extends I> inter, T impl) {}
