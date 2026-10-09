/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.lang.reflect.Method;
import java.util.List;

/**
 * Every provider that wraps another one must declare {@link StorageProvider#objectMetadata}.
 * <p>
 * The method has a default, and that default resolves through {@code this.newObject}. So a decorator that does not
 * declare it does not inherit the delegate's override — it silently takes the delegate's handle path instead, and
 * the override never runs. That is not hypothetical: an earlier metadata method on this interface was unreachable
 * for exactly this reason, and was deleted as dead code after the suites passed without it.
 * <p>
 * Classes are named as strings and loaded by name because two of them are private nested classes, and the
 * access-bypassing reflection that would reach them from source is forbidden in this repo. An override of a public
 * interface method is itself public, so {@code getMethods()} finds it on a private class just the same.
 */
public class StorageProviderMetadataForwardingTests extends ESTestCase {

    private static final List<String> WRAPPING_PROVIDERS = List.of(
        "org.elasticsearch.xpack.esql.datasources.RetryableStorageProvider",
        "org.elasticsearch.xpack.esql.datasources.QueryBudgetedStorageProvider",
        "org.elasticsearch.xpack.esql.datasources.ConcurrencyLimitedStorageProvider",
        "org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache$PooledStorageProvider",
        "org.elasticsearch.xpack.esql.datasources.FileSourceFactory$DeferredPoolLease"
    );

    public void testEveryWrappingProviderForwardsObjectMetadata() throws Exception {
        for (String name : WRAPPING_PROVIDERS) {
            Class<?> wrapper = Class.forName(name);
            assertTrue(
                name + " is listed here but does not implement StorageProvider, so this entry checks nothing",
                StorageProvider.class.isAssignableFrom(wrapper)
            );
            assertTrue(
                wrapper.getName()
                    + " wraps a StorageProvider but does not declare objectMetadata, so it inherits the interface "
                    + "default and the delegate's override never runs. Forward it to the delegate.",
                declaresObjectMetadata(wrapper)
            );
        }
    }

    private static boolean declaresObjectMetadata(Class<?> wrapper) {
        for (Method method : wrapper.getMethods()) {
            if (method.getName().equals("objectMetadata")
                && method.getParameterCount() == 1
                && method.getParameterTypes()[0] == StoragePath.class
                && method.getDeclaringClass() == wrapper) {
                return true;
            }
        }
        return false;
    }
}
