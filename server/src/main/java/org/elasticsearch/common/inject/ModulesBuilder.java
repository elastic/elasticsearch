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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class ModulesBuilder implements Iterable<Module> {

    private final List<Module> modules = new ArrayList<>();

    public ModulesBuilder add(Module... newModules) {
        Collections.addAll(modules, newModules);
        return this;
    }

    @Override
    public Iterator<Module> iterator() {
        return modules.iterator();
    }

    public Injector createInjector() {
        Injector injector = Guice.createInjector(modules);
        ((InjectorImpl) injector).clearCache();
        // in ES, we always create all instances as if they are eager singletons
        // this allows for considerable memory savings (no need to store construction info) as well as cycles
        ((InjectorImpl) injector).readOnlyAllSingletons();
        return injector;
    }
}
