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

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.elasticsearch.test.VersionUtils.getFirstVersion;
import static org.elasticsearch.test.VersionUtils.getPreviousVersion;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasToString;

public class MapperServiceTests extends ESSingleNodeTestCase {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
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

    @Test
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

    @Test
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
}
