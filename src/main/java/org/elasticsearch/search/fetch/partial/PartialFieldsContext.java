/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.search.fetch.partial;

import com.google.common.collect.Lists;

import java.util.List;

/**
 */
public class PartialFieldsContext {

    public static class PartialField {
        private final String name;
        private final String[] includes;
        private final String[] excludes;

        public PartialField(String name, String[] includes, String[] excludes) {
            this.name = name;
            this.includes = includes;
            this.excludes = excludes;
        }

        public String name() {
            return this.name;
        }

        public String[] includes() {
            return this.includes;
        }

        public String[] excludes() {
            return this.excludes;
        }
    }

    private final List<PartialField> fields = Lists.newArrayList();

    public PartialFieldsContext() {

    }

    public void add(PartialField field) {
        fields.add(field);
    }

    public List<PartialField> fields() {
        return this.fields;
    }
}
