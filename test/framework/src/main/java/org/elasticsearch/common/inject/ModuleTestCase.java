/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
