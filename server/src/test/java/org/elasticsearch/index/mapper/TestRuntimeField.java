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

package org.elasticsearch.index.mapper;

import org.apache.lucene.search.Query;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.plugins.MapperPlugin;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class TestRuntimeField extends RuntimeFieldType {

    private final String type;

    public TestRuntimeField(String name, String type) {
        super(name, Collections.emptyMap());
        this.type = type;
    }

    @Override
    protected void doXContentBody(XContentBuilder builder, boolean includeDefaults) throws IOException {
    }

    @Override
    public ValueFetcher valueFetcher(QueryShardContext context, String format) {
        return null;
    }

    @Override
    public String typeName() {
        return type;
    }

    @Override
    public Query termQuery(Object value, QueryShardContext context) {
        return null;
    }

    public static class Plugin extends org.elasticsearch.plugins.Plugin implements MapperPlugin {
        @Override
        public Map<String, Parser> getRuntimeFieldTypes() {
            return Map.of(
                "keyword", (name, node, parserContext) -> new TestRuntimeField(name, "keyword"),
                "double", (name, node, parserContext) -> new TestRuntimeField(name, "double"),
                "long", (name, node, parserContext) -> new TestRuntimeField(name, "long"),
                "boolean", (name, node, parserContext) -> new TestRuntimeField(name, "boolean"),
                "date", (name, node, parserContext) -> new TestRuntimeField(name, "date"));
        }

        @Override
        public DynamicRuntimeFieldsBuilder getDynamicRuntimeFieldsBuilder() {
            return new DynamicRuntimeFieldsBuilder() {
                @Override
                public RuntimeFieldType newDynamicStringField(String name) {
                    return new TestRuntimeField(name, "keyword");
                }

                @Override
                public RuntimeFieldType newDynamicLongField(String name) {
                    return new TestRuntimeField(name, "long");
                }

                @Override
                public RuntimeFieldType newDynamicDoubleField(String name) {
                    return new TestRuntimeField(name, "double");
                }

                @Override
                public RuntimeFieldType newDynamicBooleanField(String name) {
                    return new TestRuntimeField(name, "boolean");
                }

                @Override
                public RuntimeFieldType newDynamicDateField(String name, DateFormatter dateFormatter) {
                    return new TestRuntimeField(name, "date");
                }
            };
        }
    }
}
