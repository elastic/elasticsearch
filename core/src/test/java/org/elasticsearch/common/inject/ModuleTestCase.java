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

import org.elasticsearch.common.inject.spi.*;
import org.elasticsearch.test.ESTestCase;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Base testcase for testing {@link Module} implementations.
 */
public abstract class ModuleTestCase extends ESTestCase {

    /** Configures the module and asserts "clazz" is bound to "to". */
    public void assertBinding(Module module, Class to, Class clazz) {
        List<Element> elements = Elements.getElements(module);
        for (Element element : elements) {
            if (element instanceof LinkedKeyBinding) {
                LinkedKeyBinding binding = (LinkedKeyBinding) element;
                if (to.equals(binding.getKey().getTypeLiteral().getType())) {
                    assertSame(clazz, binding.getLinkedKey().getTypeLiteral().getType());
                    return;
                }
            } else if (element instanceof UntargettedBinding) {
                UntargettedBinding binding = (UntargettedBinding) element;
                if (to.equals(binding.getKey().getTypeLiteral().getType())) {
                    assertSame(clazz, to);
                    return;
                }
            }
        }
        StringBuilder s = new StringBuilder();
        for (Element element : elements) {
            s.append(element + "\n");
        }
        fail("Did not find any binding to " + to.getName() + ". Found these bindings:\n" + s);
    }

    /**
     * Attempts to configure the module, and asserts an {@link IllegalArgumentException} is
     * caught, containing the given messages
     */
    public void assertBindingFailure(Module module, String... msgs) {
        try {
            List<Element> elements = Elements.getElements(module);
            StringBuilder s = new StringBuilder();
            for (Element element : elements) {
                s.append(element + "\n");
            }
            fail("Expected exception from configuring module. Found these bindings:\n" + s);
        } catch (IllegalArgumentException e) {
            for (String msg : msgs) {
                assertTrue(e.getMessage() + " didn't contain: " + msg, e.getMessage().contains(msg));
            }
        }
    }

    /**
     * Configures the module and checks a Map<String, Class> of the "to" class
     * is bound to "theClass".
     */
    public void assertMapMultiBinding(Module module, Class to, Class theClass) {
        List<Element> elements = Elements.getElements(module);
        Set<Type> bindings = new HashSet<>();
        boolean providerFound = false;
        for (Element element : elements) {
            if (element instanceof LinkedKeyBinding) {
                LinkedKeyBinding binding = (LinkedKeyBinding) element;
                if (to.equals(binding.getKey().getTypeLiteral().getType())) {
                    bindings.add(binding.getLinkedKey().getTypeLiteral().getType());
                }
            } else if (element instanceof ProviderInstanceBinding) {
                ProviderInstanceBinding binding = (ProviderInstanceBinding) element;
                String setType = binding.getKey().getTypeLiteral().getType().toString();
                if (setType.equals("java.util.Map<java.lang.String, " + to.getName() + ">")) {
                    providerFound = true;
                }
            }
        }

        if (bindings.contains(theClass) == false) {
            fail("Expected to find " + theClass.getName() + " as binding to " + to.getName() + ", found these classes:\n" + bindings);
        }
        assertTrue("Did not find provider for map of " + to.getName(), providerFound);
    }


    /**
     * Configures the module and checks a Set of the "to" class
     * is bound to "classes". There may be more classes bound
     * to "to" than just "classes".
     */
    public void assertSetMultiBinding(Module module, Class to, Class... classes) {
        List<Element> elements = Elements.getElements(module);
        Set<Type> bindings = new HashSet<>();
        boolean providerFound = false;
        for (Element element : elements) {
            if (element instanceof LinkedKeyBinding) {
                LinkedKeyBinding binding = (LinkedKeyBinding) element;
                if (to.equals(binding.getKey().getTypeLiteral().getType())) {
                    bindings.add(binding.getLinkedKey().getTypeLiteral().getType());
                }
            } else if (element instanceof ProviderInstanceBinding) {
                ProviderInstanceBinding binding = (ProviderInstanceBinding) element;
                String setType = binding.getKey().getTypeLiteral().getType().toString();
                if (setType.equals("java.util.Set<" + to.getName() + ">")) {
                    providerFound = true;
                }
            }
        }

        for (Class clazz : classes) {
            if (bindings.contains(clazz) == false) {
                fail("Expected to find " + clazz.getName() + " as set binding to " + to.getName() + ", found these classes:\n" + bindings);
            }
        }
        assertTrue("Did not find provider for set of " + to.getName(), providerFound);
    }

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
    public <T> void assertInstanceBindingWithAnnotation(Module module, Class<T> to, Predicate<T> tester, Class<? extends Annotation> annotation) {
        List<Element> elements = Elements.getElements(module);
        for (Element element : elements) {
            if (element instanceof InstanceBinding) {
                InstanceBinding binding = (InstanceBinding) element;
                if (to.equals(binding.getKey().getTypeLiteral().getType())) {
                    if (annotation == null || annotation.equals(binding.getKey().getAnnotationType())) {
                        assertTrue(tester.test(to.cast(binding.getInstance())));
                        return;
                    }
                }
            }
        }
        StringBuilder s = new StringBuilder();
        for (Element element : elements) {
            s.append(element + "\n");
        }
        fail("Did not find any instance binding to " + to.getName() + ". Found these bindings:\n" + s);
    }

    /**
     * Configures the module, and ensures a map exists between the "keyType" and "valueType",
     * and that all of the "expected" values are bound.
     */
    @SuppressWarnings("unchecked")
    public <K, V> void assertMapInstanceBinding(Module module, Class<K> keyType, Class<V> valueType, Map<K, V> expected) throws Exception {
        // this method is insane because java type erasure makes it incredibly difficult...
        Map<K, Key> keys = new HashMap<>();
        Map<Key, V> values = new HashMap<>();
        List<Element> elements = Elements.getElements(module);
        for (Element element : elements) {
            if (element instanceof InstanceBinding) {
                InstanceBinding binding = (InstanceBinding) element;
                if (binding.getKey().getRawType().equals(valueType)) {
                    values.put(binding.getKey(), (V) binding.getInstance());
                } else if (binding.getInstance() instanceof Map.Entry) {
                    Map.Entry entry = (Map.Entry) binding.getInstance();
                    Object key = entry.getKey();
                    Object providerValue = entry.getValue();
                    if (key.getClass().equals(keyType) && providerValue instanceof ProviderLookup.ProviderImpl) {
                        ProviderLookup.ProviderImpl provider = (ProviderLookup.ProviderImpl) providerValue;
                        keys.put((K) key, provider.getKey());
                    }
                }
            }
        }
        for (Map.Entry<K, V> entry : expected.entrySet()) {
            Key valueKey = keys.get(entry.getKey());
            assertNotNull("Could not find binding for key [" + entry.getKey() + "], found these keys:\n" + keys.keySet(), valueKey);
            V value = values.get(valueKey);
            assertNotNull("Could not find value for instance key [" + valueKey + "], found these bindings:\n" + elements);
            assertEquals(entry.getValue(), value);
        }
    }
}
