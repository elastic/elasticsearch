
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
import org.elasticsearch.common.inject.spi.LinkedKeyBinding;
import org.elasticsearch.common.inject.spi.ProviderInstanceBinding;
import org.elasticsearch.test.ESTestCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Base testcase for testing {@link Module} implementations.
 */
public abstract class ModuleTestCase extends ESTestCase {

    /** Configures the module and asserts "clazz" is bound to "to". */
    public void assertBinding(Module module, Class to, Class clazz) {
        List<Element> elements = Elements.getElements(module);
        for (Element element : elements) {
            if (element instanceof LinkedKeyBinding) {
                LinkedKeyBinding binding = (LinkedKeyBinding)element;
                if (to.getName().equals(binding.getKey().getTypeLiteral().getType().getTypeName())) {
                    assertEquals(clazz.getName(), binding.getLinkedKey().getTypeLiteral().getType().getTypeName());
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
                assertTrue(e.getMessage().contains(msg));
            }
        }
    }

    /**
     * Configures the module and checks a Set of the "to" class
     * is bound to "classes". There may be more classes bound
     * to "to" than just "classes".
     */
    public void assertSetMultiBinding(Module module, Class to, Class... classes) {
        List<Element> elements = Elements.getElements(module);
        Set<String> bindings = new HashSet<>();
        boolean providerFound = false;
        for (Element element : elements) {
            if (element instanceof LinkedKeyBinding) {
                LinkedKeyBinding binding = (LinkedKeyBinding)element;
                if (to.getName().equals(binding.getKey().getTypeLiteral().getType().getTypeName())) {
                    bindings.add(binding.getLinkedKey().getTypeLiteral().getType().getTypeName());
                }
            } else if (element instanceof ProviderInstanceBinding) {
                ProviderInstanceBinding binding = (ProviderInstanceBinding)element;
                String setType = binding.getKey().getTypeLiteral().getType().getTypeName();
                if (setType.equals("java.util.Set<" + to.getName() + ">")) {
                    providerFound = true;
                }
            }
        }

        for (Class clazz : classes) {
            if (bindings.contains(clazz.getName()) == false) {
                fail("Expected to find " + clazz.getName() + " as set binding to " + to.getName() + ", found these classes:\n" + bindings);
            }
        }
        assertTrue("Did not find provider for set of " + to.getName(), providerFound);
    }

    // TODO: add assert for map multibinding
}
