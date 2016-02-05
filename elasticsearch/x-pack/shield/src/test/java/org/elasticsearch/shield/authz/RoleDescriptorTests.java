/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

public class RoleDescriptorTests extends ESTestCase {

    public void testIndexGroup() throws Exception {
        RoleDescriptor.IndicesPrivileges privs = RoleDescriptor.IndicesPrivileges.builder()
                .indices(new String[]{"idx"})
                .privileges(new String[]{"priv"})
                .build();
        XContentBuilder b = jsonBuilder();
        privs.toXContent(b, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"names\":[\"idx\"],\"privileges\":[\"priv\"]}", b.string());
    }

    public void testRDJson() throws Exception {
        List<RoleDescriptor.IndicesPrivileges> groups = new ArrayList<>();
        groups.add(RoleDescriptor.IndicesPrivileges.builder()
                .indices(new String[]{"i1", "i2"})
                .privileges(new String[]{"read"})
                .fields(new String[]{"body", "title"})
                .query(new BytesArray("{\"query\": {\"match_all\": {}}}"))
                .build());
        RoleDescriptor d = new RoleDescriptor("test", new String[]{"all", "none"}, groups, new String[]{"sudo"});
        assertEquals("Role[name=test, cluster=[all,none], indicesPrivileges=[IndicesPrivileges[privileges=[read], indices=[i1,i2], " +
                "fields=[body,title], query={\"query\": {\"match_all\": {}}}],], runAs=[sudo]]", d.toString());
        XContentBuilder builder = jsonBuilder();
        d.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"name\":\"test\",\"cluster\":[\"all\",\"none\"],\"indices\":[{\"names\":[\"i1\",\"i2\"]," +
                "\"privileges\":[\"read\"],\"fields\":[\"body\",\"title\"],\"query\":\"{\\\"query\\\": {\\\"match_all\\\": {}}}\"}]," +
                "\"run_as\":[\"sudo\"]}",
                builder.string());
    }

    public void testRDParsing() throws Exception {
        String q;
        RoleDescriptor rd;
        try {
            q = "{}";
            rd = RoleDescriptor.source(new BytesArray(q));
            fail("should have failed");
        } catch (ElasticsearchParseException e) {
            // expected
        }

        q = "{\"name\": \"test\", \"cluster\":[\"a\", \"b\"]}";
        rd = RoleDescriptor.source(new BytesArray(q));
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[]{"a", "b"}, rd.getClusterPattern());
        assertEquals(0, rd.getIndicesPrivileges().size());
        assertArrayEquals(Strings.EMPTY_ARRAY, rd.getRunAs());

        q = "{\"name\": \"test\", \"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"]}";
        rd = RoleDescriptor.source(new BytesArray(q));
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[]{"a", "b"}, rd.getClusterPattern());
        assertEquals(0, rd.getIndicesPrivileges().size());
        assertArrayEquals(new String[]{"m", "n"}, rd.getRunAs());

        q = "{\"name\": \"test\", \"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"], \"indices\": [{\"names\": \"idx1\", " +
                "\"privileges\": [\"p1\", \"p2\"]}, {\"names\": \"idx2\", \"privileges\": [\"p3\"], \"fields\": [\"f1\", \"f2\"]}, " +
                "{\"names\": \"idx2\", \"privileges\": [\"p3\"], \"fields\": [\"f1\", \"f2\"], \"query\": \"{\\\"match_all\\\": {}}\"}]}";
        rd = RoleDescriptor.source(new BytesArray(q));
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[]{"a", "b"}, rd.getClusterPattern());
        assertEquals(3, rd.getIndicesPrivileges().size());
        assertArrayEquals(new String[]{"m", "n"}, rd.getRunAs());

        q = "{\"name\": \"test\", \"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"], \"indices\": [{\"names\": [\"idx1\",\"idx2\"], " +
                "\"privileges\": [\"p1\", \"p2\"]}]}";
        rd = RoleDescriptor.source(new BytesArray(q));
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[]{"a", "b"}, rd.getClusterPattern());
        assertEquals(1, rd.getIndicesPrivileges().size());
        assertArrayEquals(new String[]{"idx1", "idx2"}, rd.getIndicesPrivileges().get(0).getIndices());
        assertArrayEquals(new String[]{"m", "n"}, rd.getRunAs());

        try {
            q = "{\"name\": \"test\", \"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"], \"indices\": [{\"names\": \"idx1,idx2\", " +
                    "\"privileges\": [\"p1\", \"p2\"]}]}";
            rd = RoleDescriptor.source(new BytesArray(q));
            fail("should have thrown a parse exception");
        } catch (ElasticsearchParseException epe) {
            assertTrue(epe.getMessage(),
                    epe.getMessage().contains("index name [idx1,idx2] may not contain ','"));
        }

        try {
            // Same, but an array of names
            q = "{\"name\": \"test\", \"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"], \"indices\": [{\"names\": [\"idx1,idx2\"], " +
                    "\"privileges\": [\"p1\", \"p2\"]}]}";
            rd = RoleDescriptor.source(new BytesArray(q));
            fail("should have thrown a parse exception");
        } catch (ElasticsearchParseException epe) {
            assertTrue(epe.getMessage(),
                    epe.getMessage().contains("index name [idx1,idx2] may not contain ','"));
        }
    }
}
