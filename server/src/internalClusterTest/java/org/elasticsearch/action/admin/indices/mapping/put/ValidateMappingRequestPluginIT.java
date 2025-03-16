/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
            return Collections.singletonList((request, projectMetadata, indices) -> {
                for (Index index : indices) {
                    if (allowedOrigins.getOrDefault(index.getName(), Collections.emptySet()).contains(request.origin()) == false) {
                        return Optional.of(
                            new IllegalStateException("not allowed: index[" + index.getName() + "] origin[" + request.origin() + "]")
                        );
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
            PutMappingRequest request = new PutMappingRequest().indices("index_1").source("t1", "type=keyword").origin(origin);
            Exception e = expectThrows(IllegalStateException.class, indicesAdmin().putMapping(request));
            assertThat(e.getMessage(), equalTo("not allowed: index[index_1] origin[" + origin + "]"));
        }
        {
            PutMappingRequest request = new PutMappingRequest().indices("index_1")
                .origin(randomFrom("1", "2"))
                .source("t1", "type=keyword");
            assertAcked(indicesAdmin().putMapping(request).actionGet());
        }

        {
            String origin = randomFrom("", "1", "4", "5");
            PutMappingRequest request = new PutMappingRequest().indices("index_2").source("t2", "type=keyword").origin(origin);
            Exception e = expectThrows(IllegalStateException.class, indicesAdmin().putMapping(request));
            assertThat(e.getMessage(), equalTo("not allowed: index[index_2] origin[" + origin + "]"));
        }
        {
            PutMappingRequest request = new PutMappingRequest().indices("index_2")
                .origin(randomFrom("2", "3"))
                .source("t1", "type=keyword");
            assertAcked(indicesAdmin().putMapping(request).actionGet());
        }

        {
            String origin = randomFrom("", "1", "3", "4");
            PutMappingRequest request = new PutMappingRequest().indices("*").source("t3", "type=keyword").origin(origin);
            Exception e = expectThrows(IllegalStateException.class, indicesAdmin().putMapping(request));
            assertThat(e.getMessage(), containsString("not allowed:"));
        }
        {
            PutMappingRequest request = new PutMappingRequest().indices("index_2").origin("2").source("t3", "type=keyword");
            assertAcked(indicesAdmin().putMapping(request).actionGet());
        }
    }
}
