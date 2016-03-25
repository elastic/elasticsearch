/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.core.Is.is;

public class RoleDescriptorTests extends ESTestCase {

    public void testIndexGroup() throws Exception {
        RoleDescriptor.IndicesPrivileges privs = RoleDescriptor.IndicesPrivileges.builder()
                .indices("idx")
                .privileges("priv")
                .build();
        XContentBuilder b = jsonBuilder();
        privs.toXContent(b, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"names\":[\"idx\"],\"privileges\":[\"priv\"]}", b.string());
    }

    public void testToString() throws Exception {
        RoleDescriptor.IndicesPrivileges[] groups = new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("i1", "i2")
                        .privileges("read")
                        .fields("body", "title")
                        .query("{\"query\": {\"match_all\": {}}}")
                        .build()
        };
        RoleDescriptor descriptor = new RoleDescriptor("test", new String[] { "all", "none" }, groups, new String[] { "sudo" });
        assertThat(descriptor.toString(), is("Role[name=test, cluster=[all,none], indicesPrivileges=[IndicesPrivileges[indices=[i1,i2], " +
                "privileges=[read], fields=[body,title], query={\"query\": {\"match_all\": {}}}],], runAs=[sudo]]"));
    }

    public void testToXContent() throws Exception {
        RoleDescriptor.IndicesPrivileges[] groups = new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("i1", "i2")
                        .privileges("read")
                        .fields("body", "title")
                        .query("{\"query\": {\"match_all\": {}}}")
                        .build()
        };
        RoleDescriptor descriptor = new RoleDescriptor("test", new String[] { "all", "none" }, groups, new String[] { "sudo" });
        XContentBuilder builder = descriptor.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS);
        RoleDescriptor parsed = RoleDescriptor.parse("test", builder.bytes());
        assertThat(parsed, is(descriptor));
    }

    public void testParse() throws Exception {

        String q = "{\"cluster\":[\"a\", \"b\"]}";
        RoleDescriptor rd = RoleDescriptor.parse("test", new BytesArray(q));
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(0, rd.getIndicesPrivileges().length);
        assertArrayEquals(Strings.EMPTY_ARRAY, rd.getRunAs());

        q = "{\"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q));
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(0, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());

        q = "{\"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"], \"indices\": [{\"names\": \"idx1\", \"privileges\": [\"p1\", " +
                "\"p2\"]}, {\"names\": \"idx2\", \"privileges\": [\"p3\"], \"fields\": [\"f1\", \"f2\"]}, {\"names\": \"idx2\", " +
                "\"privileges\": [\"p3\"], \"fields\": [\"f1\", \"f2\"], \"query\": \"{\\\"match_all\\\": {}}\"}]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q));
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(3, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());

        q = "{\"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"], \"indices\": [{\"names\": [\"idx1\",\"idx2\"], \"privileges\": " +
                "[\"p1\", \"p2\"]}]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q));
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(1, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "idx1", "idx2" }, rd.getIndicesPrivileges()[0].getIndices());
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());
    }
}
