/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.util.guice;

import com.google.inject.AbstractModule;
import com.google.inject.BindingAnnotation;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.matcher.Matchers;
import org.testng.annotations.Test;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.*;
import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

/**
 * @author kimchy (Shay Banon)
 */
public class InjectorsTests {

    @Test public void testMatchers() throws Exception {
        Injector injector = Guice.createInjector(new MyModule());

        assertThat(Injectors.getInstancesOf(injector, A.class).size(), equalTo(2));
        assertThat(Injectors.getInstancesOf(injector, B.class).size(), equalTo(1));
        assertThat(Injectors.getInstancesOf(injector, C.class).size(), equalTo(1));

        assertThat(Injectors.getInstancesOf(injector,
                Matchers.subclassesOf(C.class).and(Matchers.annotatedWith(Blue.class))).size(), equalTo(1));
    }

    public static class MyModule extends AbstractModule {
        protected void configure() {
            bind(C.class);
            bind(B.class);
        }
    }

    public static class A {
        public String name = "A";
    }

    public static class B extends A {
        public B() {
            name = "B";
        }
    }

    @Blue
    public static class C extends A {
        public C() {
            name = "C";
        }
    }

    @Target({METHOD, CONSTRUCTOR, FIELD, TYPE})
    @Retention(RUNTIME)
    @Documented
    @BindingAnnotation
    public @interface Blue {
    }
}
