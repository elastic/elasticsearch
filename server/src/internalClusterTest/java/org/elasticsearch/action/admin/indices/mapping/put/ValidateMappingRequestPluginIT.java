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

package org.elasticsearch.action.admin.indices.mapping.put;

import org.elasticsearch.action.RequestValidators;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ValidateMappingRequestPluginIT extends ESSingleNodeTestCase {
    static final Map<String, Collection<String>> allowedOrigins = ConcurrentCollections.newConcurrentMap();
    public static class TestPlugin extends Plugin implements ActionPlugin {
        @Override
        public Collection<RequestValidators.RequestValidator<PutMappingRequest>> mappingRequestValidators() {
            return Collections.singletonList((request, state, indices) -> {
                for (Index index : indices) {
                    if (allowedOrigins.getOrDefault(index.getName(), Collections.emptySet()).contains(request.origin()) == false) {
                        return Optional.of(
                                new IllegalStateException("not allowed: index[" + index.getName() + "] origin[" + request.origin() + "]"));
                    }
                }
                return Optional.empty();
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(TestPlugin.class);
    }

    public void testValidateMappingRequest() {
        createIndex("index_1");
        createIndex("index_2");
        allowedOrigins.put("index_1", Arrays.asList("1", "2"));
        allowedOrigins.put("index_2", Arrays.asList("2", "3"));
        {
            String origin = randomFrom("", "3", "4", "5");
            PutMappingRequest request = new PutMappingRequest().indices("index_1").type("doc").source("t1", "type=keyword").origin(origin);
            Exception e = expectThrows(IllegalStateException.class, () -> client().admin().indices().putMapping(request).actionGet());
            assertThat(e.getMessage(), equalTo("not allowed: index[index_1] origin[" + origin + "]"));
        }
        {
            PutMappingRequest request = new PutMappingRequest().indices("index_1").origin(randomFrom("1", "2"))
                .type("doc").source("t1", "type=keyword");
            assertAcked(client().admin().indices().putMapping(request).actionGet());
        }

        {
            String origin = randomFrom("", "1", "4", "5");
            PutMappingRequest request = new PutMappingRequest().indices("index_2").type("doc").source("t2", "type=keyword").origin(origin);
            Exception e = expectThrows(IllegalStateException.class, () -> client().admin().indices().putMapping(request).actionGet());
            assertThat(e.getMessage(), equalTo("not allowed: index[index_2] origin[" + origin + "]"));
        }
        {
            PutMappingRequest request = new PutMappingRequest().indices("index_2").origin(randomFrom("2", "3"))
                .type("doc").source("t1", "type=keyword");
            assertAcked(client().admin().indices().putMapping(request).actionGet());
        }

        {
            String origin = randomFrom("", "1", "3", "4");
            PutMappingRequest request = new PutMappingRequest().indices("*").type("doc").source("t3", "type=keyword").origin(origin);
            Exception e = expectThrows(IllegalStateException.class, () -> client().admin().indices().putMapping(request).actionGet());
            assertThat(e.getMessage(), containsString("not allowed:"));
        }
        {
            PutMappingRequest request = new PutMappingRequest().indices("index_2").origin("2")
                .type("doc").source("t3", "type=keyword");
            assertAcked(client().admin().indices().putMapping(request).actionGet());
        }
    }
}
