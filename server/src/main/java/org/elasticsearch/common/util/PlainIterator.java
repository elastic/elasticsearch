/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class PlainIterator<T> implements Iterable<T>, Countable {
    private final List<T> elements;

    // Calls to nextOrNull might be performed on different threads in the transport actions so we need the volatile
    // keyword in order to ensure visibility. Note that it is fine to use `volatile` for a counter in that case given
    // that although nextOrNull might be called from different threads, it can never happen concurrently.
    private volatile int index;

    public PlainIterator(List<T> elements) {
        this.elements = elements;
        reset();
    }

    public void reset() {
        index = 0;
    }

    public int remaining() {
        return elements.size() - index;
    }

    public T nextOrNull() {
        if (index == elements.size()) {
            return null;
        } else {
            return elements.get(index++);
        }
    }

    @Override
    public int size() {
        return elements.size();
    }


    public List<T> asList() {
        return Collections.unmodifiableList(elements);
    }

    @Override
    public Iterator<T> iterator() {
        return elements.iterator();
    }
}
