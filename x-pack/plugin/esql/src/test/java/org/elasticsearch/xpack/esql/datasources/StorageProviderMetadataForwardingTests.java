/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.cache.StorageProviderCache;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Every provider that wraps another one must forward {@link StorageProvider#objectMetadata}.
 * <p>
 * The method has a default, and that default resolves through {@code this.newObject}. So a decorator that does not
 * declare it does not inherit the delegate's override — it silently takes the delegate's handle path instead, and
 * the override never runs. That is not hypothetical: an earlier metadata method on this interface was unreachable
 * for exactly this reason, and was deleted as dead code after the suites passed without it.
 */
public class StorageProviderMetadataForwardingTests extends ESTestCase {

    public void testEveryWrappingProviderForwardsObjectMetadata() {
        List<Class<?>> wrappers = new ArrayList<>(
            List.of(RetryableStorageProvider.class, QueryBudgetedStorageProvider.class, ConcurrencyLimitedStorageProvider.class)
        );
        wrappers.addAll(nestedProvidersOf(StorageProviderCache.class));
        wrappers.addAll(nestedProvidersOf(FileSourceFactory.class));

        assertThat("found no wrapping providers to check, so this test proves nothing", wrappers.size(), greaterThanOrEqualTo(4));

        for (Class<?> wrapper : wrappers) {
            try {
                wrapper.getDeclaredMethod("objectMetadata", StoragePath.class);
            } catch (NoSuchMethodException e) {
                fail(
                    wrapper.getName()
                        + " wraps a StorageProvider but does not declare objectMetadata, so it inherits the interface "
                        + "default and the delegate's override never runs. Forward it to the delegate."
                );
            }
        }
    }

    private static List<Class<?>> nestedProvidersOf(Class<?> outer) {
        List<Class<?>> found = new ArrayList<>();
        for (Class<?> nested : outer.getDeclaredClasses()) {
            if (StorageProvider.class.isAssignableFrom(nested) && Modifier.isAbstract(nested.getModifiers()) == false) {
                found.add(nested);
            }
        }
        return found;
    }
}
