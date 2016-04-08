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

/**
 * Holds a value that is either:
 * a) set implicitly e.g. through some default value
 * b) set explicitly e.g. from a user selection
 * 
 * When merging conflicting configuration settings such as
 * field mapping settings it is preferable to preserve an explicit
 * choice rather than a choice made only made implicitly by defaults. 
 * 
 */
public class Explicit<T> {

    private final T value;
    private final boolean explicit;
    /**
     * Create a value with an indication if this was an explicit choice
     * @param value a setting value
     * @param explicit true if the value passed is a conscious decision, false if using some kind of default
     */
    public Explicit(T value, boolean explicit) {
        this.value = value;
        this.explicit = explicit;
    }

    public T value() {
        return this.value;
    }

    /**
     * 
     * @return true if the value passed is a conscious decision, false if using some kind of default
     */
    public boolean explicit() {
        return this.explicit;
    }
}
