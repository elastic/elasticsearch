/*
 * Copyright (C) 2009 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.common.inject;

import org.elasticsearch.common.inject.internal.Errors;
import org.elasticsearch.common.inject.spi.Element;

import java.util.ArrayList;
import java.util.List;

/**
 * Returns providers and members injectors that haven't yet been initialized. As a part of injector
 * creation it's necessary to {@link #initialize initialize} these lookups.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
class DeferredLookups implements Lookups {
    private final InjectorImpl injector;
    private final List<Element> lookups = new ArrayList<>();

    DeferredLookups(InjectorImpl injector) {
        this.injector = injector;
    }

    /**
     * Initialize the specified lookups, either immediately or when the injector is created.
     */
    public void initialize(Errors errors) {
        injector.lookups = injector;
        new LookupProcessor(errors).process(injector, lookups);
    }

}
