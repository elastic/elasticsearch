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

package org.elasticsearch.common;

import org.elasticsearch.test.ESTestCase;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class ReflectiveInstantiatorTests extends ESTestCase {
    public void testBuildObjectWithoutCtor() {
        ReflectiveInstantiator ri = new ReflectiveInstantiator();
        NoCtor noCtor = ri.instantiate(NoCtor.class, emptySet());
        // Building it twice gets you the same instance
        assertThat(ri.instantiate(NoCtor.class, emptySet()), sameInstance(noCtor));
    }

    public void testBuildWithCtorArg() {
        ReflectiveInstantiator ri = new ReflectiveInstantiator();
        NoCtor noCtor = new NoCtor();
        ri.addCtorArg(noCtor);
        Simple simple = ri.instantiate(Simple.class, emptySet()); 
        assertThat(simple.noCtor, sameInstance(noCtor));
    }

    public void testBuildWithPreBuilt() {
        ReflectiveInstantiator ri = new ReflectiveInstantiator();
        NoCtor noCtor = ri.instantiate(NoCtor.class, emptySet());
        Simple simple = ri.instantiate(Simple.class, singleton(NoCtor.class));
        assertThat(simple.noCtor, sameInstance(noCtor));
    }

    public void testBuildWithPreBuiltNoAllowed() {
        ReflectiveInstantiator ri = new ReflectiveInstantiator();
        ri.instantiate(NoCtor.class, emptySet());
        Throwable t = expectThrows(RuntimeException.class, () -> ri.instantiate(Simple.class, emptySet()));
        assertThat(t.getMessage(), startsWith("Error building [org.elasticsearch.common.ReflectiveInstantiatorTests$Simple]:"));
        assertThat(t.getCause(), instanceOf(IllegalArgumentException.class));
        t = t.getCause();
        assertEquals("Attempting to reuse a [org.elasticsearch.common.ReflectiveInstantiatorTests$NoCtor] but that hasn't been "
                + "explicitly permitted.", t.getMessage());
    }

    public void testDuplicateCtorArgument() {
        ReflectiveInstantiator ri = new ReflectiveInstantiator();
        NoCtor noCtor = new NoCtor();
        ri.addCtorArg(noCtor);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ri.addCtorArg(noCtor));
        assertThat(e.getMessage(), startsWith(
                "Attempted to register a duplicate ctor argument for [org.elasticsearch.common.ReflectiveInstantiatorTests$NoCtor]."));
        assertThat(e.getMessage(), containsString("Was [org.elasticsearch.common.ReflectiveInstantiatorTests$NoCtor@"));
        assertThat(e.getMessage(),
                containsString("and attempted to register [org.elasticsearch.common.ReflectiveInstantiatorTests$NoCtor@"));
    }

    public static class NoCtor {}
    public static class Simple {
        private final NoCtor noCtor;
        public Simple(NoCtor noCtor) {
            this.noCtor = noCtor;
        }
    }
}
