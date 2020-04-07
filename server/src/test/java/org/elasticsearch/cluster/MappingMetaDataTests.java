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

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;

public class MappingMetaDataTests extends ESTestCase {

    public void testEmptyMappings() throws IOException {
        MappingMetadata mmd = new MappingMetadata(new BytesArray("{\"_doc\":{}}"));
        assertEquals("_doc", mmd.type());
        assertEquals(new CompressedXContent("{\"_doc\":{}}"), mmd.source());
        assertFalse(mmd.routingRequired());
        assertEquals(MappingMetadata.EMPTY_MAPPINGS, mmd);
    }

    public void testRoutingRequired() throws IOException {
        String mappings = "{\"_doc\":{\"_source\":\"enabled\",\"_routing\":{\"required\":true},\"properties\":{}}}";
        MappingMetadata mmd = new MappingMetadata(new BytesArray(mappings));
        assertTrue(mmd.routingRequired());
        assertEquals("_doc", mmd.type());
        assertEquals(new CompressedXContent(mappings), mmd.source());
    }

    public void testNoType() {
        IllegalStateException e = expectThrows(IllegalStateException.class,
            () -> new MappingMetadata(new BytesArray("{}")));
        assertEquals("Mappings must contain a single type root", e.getMessage());
    }

    public void testFromMapWithType() {
        Map<String, Object> map = Map.of("_doc",
            Map.of("_source", "enabled", "_routing", Map.of("required", "true"), "properties", Collections.emptyMap()));
        MappingMetadata mmd = new MappingMetadata("_doc", map);
        assertEquals("_doc", mmd.type());
        assertTrue(mmd.routingRequired());
        String parsedMappings = mmd.source().string();
        assertThat(parsedMappings, containsString("\"properties\":{}"));
        assertThat(parsedMappings, containsString("\"_source\":\"enabled\""));
        assertThat(parsedMappings, containsString("\"_routing\":{\"required\":\"true\"}"));
    }

    public void testFromMapNoType() {
        Map<String, Object> map
            = Map.of("_source", "enabled", "_routing", Map.of("required", "true"), "properties", Collections.emptyMap());
        MappingMetadata mmd = new MappingMetadata("_doc", map);
        assertEquals("_doc", mmd.type());
        assertTrue(mmd.routingRequired());
        String parsedMappings = mmd.source().string();
        assertThat(parsedMappings, containsString("\"properties\":{}"));
        assertThat(parsedMappings, containsString("\"_source\":\"enabled\""));
        assertThat(parsedMappings, containsString("\"_routing\":{\"required\":\"true\"}"));
    }

    public void testFromMapNoTypeSingleton() throws IOException {
        Map<String, Object> map = Map.of("properties", Map.of("field", "type"));
        MappingMetadata mmd = new MappingMetadata("_doc", map);
        assertEquals("_doc", mmd.type());
        assertFalse(mmd.routingRequired());
        assertEquals(new CompressedXContent("{\"_doc\":{\"properties\":{\"field\":\"type\"}}}"), mmd.source());
    }

}
