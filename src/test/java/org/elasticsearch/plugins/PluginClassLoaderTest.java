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
package org.elasticsearch.plugins;

import org.junit.Before;
import org.junit.Test;

import java.net.URL;
import java.util.Properties;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.*;


public class PluginClassLoaderTest {

    private static final String NAME = "es.test.isolated.plugin.name";
    private static final String INSTANCE = "es.test.isolated.plugin.instantiated";
    private static final String READ = "es.test.isolated.plugin.read.name";

    private URL root;
    private String clazz = getClass().getName().replace('.', '/').concat(".class");
    private ClassLoader parent;

    @Before
    public void before() throws Exception {
        System.getProperties().setProperty(READ, "");
        System.getProperties().setProperty(NAME, "");
        System.getProperties().setProperty(INSTANCE, "");

        parent = getClass().getClassLoader();
        URL url = parent.getResource(clazz);
        root = new URL(url.toString().substring(0, url.toString().indexOf(clazz)));
    }

    @Test
    public void testClassSpaceIsolationEvenWhenPointingToSameClass() throws Exception {

        PluginClassLoader space1 = new PluginClassLoader(new URL[] { root }, null);
        Properties props = System.getProperties();
        assertThat(props.getProperty(READ), is(""));
        props.setProperty(NAME, "one");
        Class<?> class1 = Class.forName("org.elasticsearch.plugins.isolation.DummyClass", true, space1);
        assertThat(props.getProperty(READ), is("one"));

        String instance1 = props.getProperty(INSTANCE);

        PluginClassLoader space2 = new PluginClassLoader(new URL[] { root }, null);
        props.setProperty(NAME, "two");
        Class<?> class2 = Class.forName("org.elasticsearch.plugins.isolation.DummyClass", true, space2);
        assertThat(props.getProperty(READ), is("two"));

        String instance2 = props.getProperty(INSTANCE);
        assertThat(instance1, not(instance2));
        assertNotSame(class1, class2);
    }

    @Test
    public void testDelegateToParentIfResourcesNotFoundLocally() throws Exception {

        PluginClassLoader space1 = new PluginClassLoader(new URL[] {}, null);
        assertNull(space1.getResource(clazz));

        PluginClassLoader space2 = new PluginClassLoader(new URL[] {}, parent);
        assertNotNull(parent.getResource(clazz));
        assertNotNull(space2.getResource(clazz));
    }

    @Test
    public void testIgnoreClassesInParentIfFoundLocally() throws Exception {
        ClassLoader parent = getClass().getClassLoader();

        PluginClassLoader space1 = new PluginClassLoader(new URL[] { root }, parent);

        Properties props = System.getProperties();
        assertThat(props.getProperty(READ), is(""));
        props.setProperty(NAME, "one");

        String before = props.getProperty(NAME);
        Class parentClass = Class.forName("org.elasticsearch.plugins.isolation.DummyClass", true, parent);
        props.setProperty(NAME, "one");
        Class class1 = Class.forName("org.elasticsearch.plugins.isolation.DummyClass", true, space1);
        assertThat(props.getProperty(READ), is(before));
        assertThat(class1, is(not(parentClass)));
    }
}
