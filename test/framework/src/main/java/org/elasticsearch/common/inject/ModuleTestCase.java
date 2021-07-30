/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.spi.Element;
import org.elasticsearch.common.inject.spi.Elements;
import org.elasticsearch.common.inject.spi.InstanceBinding;
import org.elasticsearch.common.inject.spi.ProviderInstanceBinding;
import org.elasticsearch.test.ESTestCase;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.function.Predicate;

/**
 * Base testcase for testing {@link Module} implementations.
 */
public abstract class ModuleTestCase extends ESTestCase {
    /**
     * Configures the module, and ensures an instance is bound to the "to" class, and the
     * provided tester returns true on the instance.
     */
    public <T> void assertInstanceBinding(Module module, Class<T> to, Predicate<T> tester) {
        assertInstanceBindingWithAnnotation(module, to, tester, null);
    }

    /**
     * Like {@link #assertInstanceBinding(Module, Class, Predicate)}, but filters the
     * classes checked by the given annotation.
     */
    private <T> void assertInstanceBindingWithAnnotation(Module module, Class<T> to,
            Predicate<T> tester, Class<? extends Annotation> annotation) {
        List<Element> elements = Elements.getElements(module);
        for (Element element : elements) {
            if (element instanceof InstanceBinding) {
                InstanceBinding<?> binding = (InstanceBinding<?>) element;
                if (to.equals(binding.getKey().getTypeLiteral().getType())) {
                    if (annotation == null || annotation.equals(binding.getKey().getAnnotationType())) {
                        assertTrue(tester.test(to.cast(binding.getInstance())));
                        return;
                    }
                }
            } else  if (element instanceof ProviderInstanceBinding) {
                ProviderInstanceBinding<?> binding = (ProviderInstanceBinding<?>) element;
                if (to.equals(binding.getKey().getTypeLiteral().getType())) {
                    assertTrue(tester.test(to.cast(binding.getProviderInstance().get())));
                    return;
                }
            }
        }
        StringBuilder s = new StringBuilder();
        for (Element element : elements) {
            s.append(element).append("\n");
        }
        fail("Did not find any instance binding to " + to.getName() + ". Found these bindings:\n" + s);
    }
}
