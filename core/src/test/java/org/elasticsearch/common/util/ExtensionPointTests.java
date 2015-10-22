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

import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.common.inject.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ExtensionPointTests extends ESTestCase {

    public void testClassSet() {
        final ExtensionPoint.ClassSet<AllocationDecider> allocationDeciders = new ExtensionPoint.ClassSet<>("allocation_decider", AllocationDecider.class, AllocationDeciders.class);
        allocationDeciders.registerExtension(TestAllocationDecider.class);
        Injector injector = new ModulesBuilder().add(new Module() {
            @Override
            public void configure(Binder binder) {
                binder.bind(Settings.class).toInstance(Settings.EMPTY);
                binder.bind(Consumer.class).asEagerSingleton();
                allocationDeciders.bind(binder);
            }
        }).createInjector();
        assertEquals(1, TestAllocationDecider.instances.get());

    }

    public static class Consumer {
        @Inject
        public Consumer(Set<AllocationDecider> deciders, TestAllocationDecider other) {
            // we require the TestAllocationDecider more than once to ensure it's bound as a singleton
        }
    }

    public static class TestAllocationDecider extends AllocationDecider {
        static final AtomicInteger instances = new AtomicInteger(0);

        @Inject
        public TestAllocationDecider(Settings settings) {
            super(settings);
            instances.incrementAndGet();
        }
    }
}
