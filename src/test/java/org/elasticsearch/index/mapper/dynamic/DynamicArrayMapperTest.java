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
package org.elasticsearch.index.mapper.dynamic;


import com.google.common.base.Predicate;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.*;
import org.elasticsearch.index.mapper.array.DynamicArrayFieldMapperBuilderFactory;
import org.elasticsearch.index.mapper.array.DynamicArrayFieldMapperException;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes = 1, numClientNodes = 0, enableRandomBenchNodes = false)
public class DynamicArrayMapperTest extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("plugin.types", ExternalDynamicArrayMapperPlugin.class.getName())
                .build();
    }

    @Test
    public void testInjectionOfDynamicArrayMapper() throws Exception {
        prepareCreate("test-array").addMapping("type",
                XContentFactory.jsonBuilder()
                        .startObject().startObject("type")
                        .startObject("properties")
                        .startObject("field1").field("type", "string").endObject()
                        .endObject().endObject().endObject()).execute().get();
        ensureYellow("test-array");

        ExternalDynamicArrayMapperModule.Builder.BuilderFactory factory = (ExternalDynamicArrayMapperModule.Builder.BuilderFactory) internalCluster().getInstance(DynamicArrayFieldMapperBuilderFactory.class);
        final ExternalDynamicArrayMapper mapper = new ExternalDynamicArrayMapper();
        factory.dynamicArrayMapper(mapper);

        index("test-array", "type", "1", XContentFactory.jsonBuilder()
                .startObject()
                    .startArray("field").value("a").value("b").value("c").endArray()
                .endObject());
        refresh();
        waitNoPendingTasksOnAll();

        assertThat(mapper.parsedTokens.size(), equalTo(4));
        assertThat(mapper.parsedTokens.get(0), equalTo(XContentParser.Token.START_ARRAY));
        assertThat(mapper.parsedTokens.get(1), equalTo(XContentParser.Token.VALUE_STRING));
        assertThat(mapper.parsedTokens.get(2), equalTo(XContentParser.Token.VALUE_STRING));
        assertThat(mapper.parsedTokens.get(3), equalTo(XContentParser.Token.VALUE_STRING));

        assertThat(mapper.traversedFields.size(), is(1));
        assertThat(mapper.traversedObjects.size(), is(1));

        // cluster state is serialized asynchronously, wait for it.
        awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object input) {
                return mapper.toXContentCalled >= 1;
            }
        });

        // index again, another parse run will throw DynamicArrayMapperException
        // but this will be catched
        index("test-array", "type", "2", XContentFactory.jsonBuilder()
                .startObject()
                .array("field") // empty array
                .endObject());
    }

    public static class ExternalDynamicArrayMapper implements Mapper {

        public List<XContentParser.Token> parsedTokens = new ArrayList<>();
        public List<FieldMapperListener> traversedFields = new ArrayList<>();
        public List<ObjectMapperListener> traversedObjects = new ArrayList<>();
        public int toXContentCalled = 0;

        private boolean parsedOnce;

        @Override
        public String name() {
            return "dynamic-test-mapper";
        }

        @Override
        public void parse(ParseContext context) throws IOException {
            if (parsedOnce) {
                throw new DynamicArrayFieldMapperException();
            } else {
                XContentParser parser = context.parser();
                XContentParser.Token token = parser.currentToken();
                while(token != XContentParser.Token.END_ARRAY){
                    parsedTokens.add(token);
                    token = parser.nextToken();
                }
            }
            parsedOnce = true;
        }

        @Override
        public void merge(Mapper mergeWith, MergeContext mergeContext) throws MergeMappingException { }

        @Override
        public void traverse(FieldMapperListener fieldMapperListener) {
            traversedFields.add(fieldMapperListener);
        }

        @Override
        public void traverse(ObjectMapperListener objectMapperListener) {
            traversedObjects.add(objectMapperListener);
        }

        @Override
        public void close() { }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            toXContentCalled += 1;
            return builder;
        }

    }
}
