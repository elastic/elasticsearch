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

package org.elasticsearch.index.mapper;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import static org.elasticsearch.test.VersionUtils.getFirstVersion;
import static org.elasticsearch.test.VersionUtils.getPreviousVersion;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.hasToString;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ExecutionException;

public class MapperServiceTests extends ESSingleNodeTestCase {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public void testTypeNameStartsWithIllegalDot() {
        expectedException.expect(MapperParsingException.class);
        expectedException.expect(hasToString(containsString("mapping type name [.test-type] must not start with a '.'")));
        String index = "test-index";
        String type = ".test-type";
        String field = "field";
        client()
                .admin()
                .indices()
                .prepareCreate(index)
                .addMapping(type, field, "type=string")
                .execute()
                .actionGet();
    }

    public void testThatLongTypeNameIsNotRejectedOnPreElasticsearchVersionTwo() {
        String index = "text-index";
        String field = "field";
        String type = new String(new char[256]).replace("\0", "a");

        CreateIndexResponse response =
                client()
                        .admin()
                        .indices()
                        .prepareCreate(index)
                        .setSettings(settings(randomVersionBetween(random(), getFirstVersion(), getPreviousVersion(Version.V_2_0_0_beta1))))
                        .addMapping(type, field, "type=string")
                        .execute()
                        .actionGet();
        assertNotNull(response);
    }

    public void testTypeNameTooLong() {
        String index = "text-index";
        String field = "field";
        String type = new String(new char[256]).replace("\0", "a");

        expectedException.expect(MapperParsingException.class);
        expectedException.expect(hasToString(containsString("mapping type name [" + type + "] is too long; limit is length 255 but was [256]")));
        client()
                .admin()
                .indices()
                .prepareCreate(index)
                .addMapping(type, field, "type=string")
                .execute()
                .actionGet();
    }

    public void testTypes() throws Exception {
        IndexService indexService1 = createIndex("index1");
        MapperService mapperService = indexService1.mapperService();
        assertEquals(Collections.emptySet(), mapperService.types());

        mapperService.merge("type1", new CompressedXContent("{\"type1\":{}}"), true, false);
        assertNull(mapperService.documentMapper(MapperService.DEFAULT_MAPPING));
        assertEquals(Collections.singleton("type1"), mapperService.types());

        mapperService.merge(MapperService.DEFAULT_MAPPING, new CompressedXContent("{\"_default_\":{}}"), true, false);
        assertNotNull(mapperService.documentMapper(MapperService.DEFAULT_MAPPING));
        assertEquals(Collections.singleton("type1"), mapperService.types());

        mapperService.merge("type2", new CompressedXContent("{\"type2\":{}}"), true, false);
        assertNotNull(mapperService.documentMapper(MapperService.DEFAULT_MAPPING));
        assertEquals(new HashSet<>(Arrays.asList("type1", "type2")), mapperService.types());
    }

    public void testIndexIntoDefaultMapping() throws Throwable {
        // 1. test implicit index creation
        try {
            client().prepareIndex("index1", MapperService.DEFAULT_MAPPING, "1").setSource("{").execute().get();
            fail();
        } catch (Throwable t) {
            if (t instanceof ExecutionException) {
                t = ((ExecutionException) t).getCause();
            }
            final Throwable throwable = ExceptionsHelper.unwrapCause(t);
            if (throwable instanceof IllegalArgumentException) {
                assertEquals("It is forbidden to index into the default mapping [_default_]", throwable.getMessage());
            } else {
                throw t;
            }
        }

        // 2. already existing index
        IndexService indexService = createIndex("index2");
        try {
            client().prepareIndex("index2", MapperService.DEFAULT_MAPPING, "2").setSource().execute().get();
            fail();
        } catch (Throwable t) {
            if (t instanceof ExecutionException) {
                t = ((ExecutionException) t).getCause();
            }
            final Throwable throwable = ExceptionsHelper.unwrapCause(t);
            if (throwable instanceof IllegalArgumentException) {
                assertEquals("It is forbidden to index into the default mapping [_default_]", throwable.getMessage());
            } else {
                throw t;
            }
        }
        assertFalse(indexService.mapperService().hasMapping(MapperService.DEFAULT_MAPPING));
    }
}
