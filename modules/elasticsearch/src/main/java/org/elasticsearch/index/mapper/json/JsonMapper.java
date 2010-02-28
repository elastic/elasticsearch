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

package org.elasticsearch.index.mapper.json;

import org.elasticsearch.index.mapper.FieldMapperListener;
import org.elasticsearch.index.mapper.MergeMappingException;
import org.elasticsearch.util.concurrent.NotThreadSafe;
import org.elasticsearch.util.concurrent.ThreadSafe;
import org.elasticsearch.util.json.ToJson;

import java.io.IOException;

/**
 * @author kimchy (Shay Banon)
 */
@ThreadSafe
public interface JsonMapper extends ToJson {

    @NotThreadSafe
    public static class BuilderContext {
        private final JsonPath jsonPath;

        public BuilderContext(JsonPath jsonPath) {
            this.jsonPath = jsonPath;
        }

        public JsonPath path() {
            return this.jsonPath;
        }
    }

    @NotThreadSafe
    public static abstract class Builder<T extends Builder, Y extends JsonMapper> {

        protected final String name;

        protected T builder;

        protected Builder(String name) {
            this.name = name;
        }

        public abstract Y build(BuilderContext context);
    }

    String name();

    void parse(JsonParseContext jsonContext) throws IOException;

    void merge(JsonMapper mergeWith, JsonMergeContext mergeContext) throws MergeMappingException;

    void traverse(FieldMapperListener fieldMapperListener);
}
