/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.security.support.MetadataUtils;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;

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
                        .grantedFields("body", "title")
                        .query("{\"query\": {\"match_all\": {}}}")
                        .build()
        };
        RoleDescriptor descriptor = new RoleDescriptor("test", new String[] { "all", "none" }, groups, new String[] { "sudo" });
        assertThat(descriptor.toString(), is("Role[name=test, cluster=[all,none], indicesPrivileges=[IndicesPrivileges[indices=[i1,i2], " +
                "privileges=[read], field_security=[grant=[body,title], except=null], query={\"query\": {\"match_all\": {}}}],]" +
                ", runAs=[sudo], metadata=[{}]]"));
    }

    public void testToXContent() throws Exception {
        RoleDescriptor.IndicesPrivileges[] groups = new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("i1", "i2")
                        .privileges("read")
                        .grantedFields("body", "title")
                        .query("{\"query\": {\"match_all\": {}}}")
                        .build()
        };
        Map<String, Object> metadata = randomBoolean() ? MetadataUtils.DEFAULT_RESERVED_METADATA : null;
        RoleDescriptor descriptor = new RoleDescriptor("test", new String[] { "all", "none" }, groups, new String[] { "sudo" }, metadata);
        XContentBuilder builder = descriptor.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS);
        RoleDescriptor parsed = RoleDescriptor.parse("test", builder.bytes(), false, XContentType.JSON);
        assertEquals(parsed, descriptor);
    }

    public void testParse() throws Exception {

        String q = "{\"cluster\":[\"a\", \"b\"]}";
        RoleDescriptor rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(0, rd.getIndicesPrivileges().length);
        assertArrayEquals(Strings.EMPTY_ARRAY, rd.getRunAs());

        q = "{\"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(0, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());

        q = "{\"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"], \"indices\": [{\"names\": \"idx1\", \"privileges\": [\"p1\", " +
                "\"p2\"]}, {\"names\": \"idx2\", \"privileges\": [\"p3\"], \"field_security\": " +
                "{\"grant\": [\"f1\", \"f2\"]}}, {\"names\": " +
                "\"idx2\", " +
                "\"privileges\": [\"p3\"], \"field_security\": {\"grant\": [\"f1\", \"f2\"]}, \"query\": \"{\\\"match_all\\\": {}}\"}]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(3, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());

        q = "{\"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"], \"indices\": [{\"names\": [\"idx1\",\"idx2\"], \"privileges\": " +
                "[\"p1\", \"p2\"]}]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(1, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "idx1", "idx2" }, rd.getIndicesPrivileges()[0].getIndices());
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());
        assertNull(rd.getIndicesPrivileges()[0].getQuery());

        q = "{\"cluster\":[\"a\", \"b\"], \"metadata\":{\"foo\":\"bar\"}}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(0, rd.getIndicesPrivileges().length);
        assertArrayEquals(Strings.EMPTY_ARRAY, rd.getRunAs());
        assertNotNull(rd.getMetadata());
        assertThat(rd.getMetadata().size(), is(1));
        assertThat(rd.getMetadata().get("foo"), is("bar"));
    }

    public void testSerialization() throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        RoleDescriptor.IndicesPrivileges[] groups = new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("i1", "i2")
                        .privileges("read")
                        .grantedFields("body", "title")
                        .query("{\"query\": {\"match_all\": {}}}")
                        .build()
        };
        Map<String, Object> metadata = randomBoolean() ? MetadataUtils.DEFAULT_RESERVED_METADATA : null;
        final RoleDescriptor descriptor =
                new RoleDescriptor("test", new String[] { "all", "none" }, groups, new String[] { "sudo" }, metadata);
        RoleDescriptor.writeTo(descriptor, output);
        StreamInput streamInput = ByteBufferStreamInput.wrap(BytesReference.toBytes(output.bytes()));
        final RoleDescriptor serialized = RoleDescriptor.readFrom(streamInput);
        assertEquals(descriptor, serialized);
    }

    public void testParseEmptyQuery() throws Exception {
        String json = "{\"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"], \"indices\": [{\"names\": [\"idx1\",\"idx2\"], " +
                "\"privileges\": [\"p1\", \"p2\"], \"query\": \"\"}]}";
        RoleDescriptor rd = RoleDescriptor.parse("test", new BytesArray(json), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(1, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "idx1", "idx2" }, rd.getIndicesPrivileges()[0].getIndices());
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());
        assertNull(rd.getIndicesPrivileges()[0].getQuery());
    }

    public void testParseIgnoresTransientMetadata() throws Exception {
        final RoleDescriptor descriptor = new RoleDescriptor("test", new String[] { "all" }, null, null,
                Collections.singletonMap("_unlicensed_feature", true), Collections.singletonMap("foo", "bar"));
        XContentBuilder b = jsonBuilder();
        descriptor.toXContent(b, ToXContent.EMPTY_PARAMS);
        RoleDescriptor parsed = RoleDescriptor.parse("test", b.bytes(), false, XContentType.JSON);
        assertNotNull(parsed);
        assertEquals(1, parsed.getTransientMetadata().size());
        assertEquals(true, parsed.getTransientMetadata().get("enabled"));
    }
}
