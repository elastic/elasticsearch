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

package org.elasticsearch.common.lucene.all;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class AllEntries {
    public static class Entry {
        private final String name;
        private final String value;
        private final float boost;

        public Entry(String name, String value, float boost) {
            this.name = name;
            this.value = value;
            this.boost = boost;
        }

        public String name() {
            return this.name;
        }

        public float boost() {
            return this.boost;
        }

        public String value() {
            return this.value;
        }
    }

    private final List<Entry> entries = new ArrayList<>();

    public void addText(String name, String text, float boost) {
        Entry entry = new Entry(name, text, boost);
        entries.add(entry);
    }

    public void clear() {
        this.entries.clear();
    }

    public List<Entry> entries() {
        return this.entries;
    }
}
