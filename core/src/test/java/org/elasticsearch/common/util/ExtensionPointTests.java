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
package org.elasticsearch.common.util;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.test.ESTestCase;

public class ExtensionPointTests extends ESTestCase {

    public void testClassSet() {
        final ExtensionPoint.ClassSet<TestBaseClass> allocationDeciders = new ExtensionPoint.ClassSet<>("test_class", TestBaseClass.class, Consumer.class);
        allocationDeciders.registerExtension(TestImpl.class);
        Injector injector = new ModulesBuilder().add(new Module() {
            @Override
            public void configure(Binder binder) {
                allocationDeciders.bind(binder);
            }
        }).createInjector();
        assertEquals(1, TestImpl.instances.get());

    }

    public static class TestBaseClass {}

    public static class Consumer {
        @Inject
        public Consumer(Set<TestBaseClass> deciders, TestImpl other) {
            // we require the TestImpl more than once to ensure it's bound as a singleton
        }
    }

    public static class TestImpl extends TestBaseClass {
        static final AtomicInteger instances = new AtomicInteger(0);

        @Inject
        public TestImpl() {
            instances.incrementAndGet();
        }
    }
}
