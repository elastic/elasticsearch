/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class RoleDescriptorTests extends ESTestCase {

    public void testIndexGroup() throws Exception {
        RoleDescriptor.IndicesPrivileges privs = RoleDescriptor.IndicesPrivileges.builder()
                .indices("idx")
                .privileges("priv")
                .allowRestrictedIndices(true)
                .build();
        XContentBuilder b = jsonBuilder();
        privs.toXContent(b, ToXContent.EMPTY_PARAMS);
        assertEquals("{\"names\":[\"idx\"],\"privileges\":[\"priv\"],\"allow_restricted_indices\":true}", Strings.toString(b));
    }

    public void testToString() {
        RoleDescriptor.IndicesPrivileges[] groups = new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("i1", "i2")
                        .privileges("read")
                        .grantedFields("body", "title")
                        .query("{\"query\": {\"match_all\": {}}}")
                        .build()
        };
        final RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges = {
            RoleDescriptor.ApplicationResourcePrivileges.builder()
                .application("my_app")
                .privileges("read", "write")
                .resources("*")
                .build()
        };

        final ConfigurableClusterPrivilege[] configurableClusterPrivileges = new ConfigurableClusterPrivilege[]{
            new ConfigurableClusterPrivileges.ManageApplicationPrivileges(new LinkedHashSet<>(Arrays.asList("app01", "app02")))
        };

        RoleDescriptor descriptor = new RoleDescriptor("test", new String[] { "all", "none" }, groups, applicationPrivileges,
            configurableClusterPrivileges, new String[] { "sudo" }, Collections.emptyMap(), Collections.emptyMap());

        assertThat(descriptor.toString(), is("Role[name=test, cluster=[all,none]" +
                ", global=[{APPLICATION:manage:applications=app01,app02}]" +
                ", indicesPrivileges=[IndicesPrivileges[indices=[i1,i2], allowRestrictedIndices=[false], privileges=[read]" +
                ", field_security=[grant=[body,title], except=null], query={\"query\": {\"match_all\": {}}}],]" +
                ", applicationPrivileges=[ApplicationResourcePrivileges[application=my_app, privileges=[read,write], resources=[*]],]" +
                ", runAs=[sudo], metadata=[{}]]"));
    }

    public void testToXContent() throws Exception {
        RoleDescriptor.IndicesPrivileges[] groups = new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("i1", "i2")
                        .privileges("read")
                        .grantedFields("body", "title")
                        .allowRestrictedIndices(randomBoolean())
                        .query("{\"query\": {\"match_all\": {}}}")
                        .build()
        };
        final RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges = {
            RoleDescriptor.ApplicationResourcePrivileges.builder()
                .application("my_app")
                .privileges("read", "write")
                .resources("*")
                .build()
        };
        final ConfigurableClusterPrivilege[] configurableClusterPrivileges = {
            new ConfigurableClusterPrivileges.ManageApplicationPrivileges(new LinkedHashSet<>(Arrays.asList("app01", "app02")))
        };

        Map<String, Object> metadata = randomBoolean() ? MetadataUtils.DEFAULT_RESERVED_METADATA : null;
        RoleDescriptor descriptor = new RoleDescriptor("test", new String[] { "all", "none" }, groups, applicationPrivileges,
            configurableClusterPrivileges, new String[]{ "sudo" }, metadata, Collections.emptyMap());
        XContentBuilder builder = descriptor.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS);
        RoleDescriptor parsed = RoleDescriptor.parse("test", BytesReference.bytes(builder), false, XContentType.JSON);
        assertThat(parsed, equalTo(descriptor));
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

        q = "{\"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"], \"index\": [{\"names\": \"idx1\", \"privileges\": [\"p1\", " +
                "\"p2\"]}, {\"names\": \"idx2\", \"allow_restricted_indices\": true, \"privileges\": [\"p3\"], \"field_security\": " +
                "{\"grant\": [\"f1\", \"f2\"]}}, {\"names\": " +
                "\"idx2\", \"allow_restricted_indices\": false," +
                "\"privileges\": [\"p3\"], \"field_security\": {\"grant\": [\"f1\", \"f2\"]}, \"query\": \"{\\\"match_all\\\": {}}\"}]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(3, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());

        q = "{\"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"], \"index\": [{\"names\": [\"idx1\",\"idx2\"], \"privileges\": " +
                "[\"p1\", \"p2\"], \"allow_restricted_indices\": true}]}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(1, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "idx1", "idx2" }, rd.getIndicesPrivileges()[0].getIndices());
        assertTrue(rd.getIndicesPrivileges()[0].allowRestrictedIndices());
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

        q = "{\"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"]," +
                " \"index\": [{\"names\": [\"idx1\",\"idx2\"], \"allow_restricted_indices\": false, \"privileges\": [\"p1\", \"p2\"]}]," +
                " \"applications\": [" +
                "     {\"resources\": [\"object-123\",\"object-456\"], \"privileges\":[\"read\", \"delete\"], \"application\":\"app1\"}," +
                "     {\"resources\": [\"*\"], \"privileges\":[\"admin\"], \"application\":\"app2\" }" +
                " ]," +
                " \"global\": { \"application\": { \"manage\": { \"applications\" : [ \"kibana\", \"logstash\" ] } } }" +
                "}";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertThat(rd.getName(), equalTo("test"));
        assertThat(rd.getClusterPrivileges(), arrayContaining("a", "b"));
        assertThat(rd.getIndicesPrivileges().length, equalTo(1));
        assertThat(rd.getIndicesPrivileges()[0].getIndices(), arrayContaining("idx1", "idx2"));
        assertThat(rd.getIndicesPrivileges()[0].allowRestrictedIndices(), is(false));
        assertThat(rd.getIndicesPrivileges()[0].getQuery(), nullValue());
        assertThat(rd.getRunAs(), arrayContaining("m", "n"));
        assertThat(rd.getApplicationPrivileges().length, equalTo(2));
        assertThat(rd.getApplicationPrivileges()[0].getResources(), arrayContaining("object-123", "object-456"));
        assertThat(rd.getApplicationPrivileges()[0].getPrivileges(), arrayContaining("read", "delete"));
        assertThat(rd.getApplicationPrivileges()[0].getApplication(), equalTo("app1"));
        assertThat(rd.getApplicationPrivileges()[1].getResources(), arrayContaining("*"));
        assertThat(rd.getApplicationPrivileges()[1].getPrivileges(), arrayContaining("admin"));
        assertThat(rd.getApplicationPrivileges()[1].getApplication(), equalTo("app2"));
        assertThat(rd.getConditionalClusterPrivileges(), Matchers.arrayWithSize(1));

        final ConfigurableClusterPrivilege conditionalPrivilege = rd.getConditionalClusterPrivileges()[0];
        assertThat(conditionalPrivilege.getCategory(), equalTo(ConfigurableClusterPrivilege.Category.APPLICATION));
        assertThat(conditionalPrivilege, instanceOf(ConfigurableClusterPrivileges.ManageApplicationPrivileges.class));
        assertThat(((ConfigurableClusterPrivileges.ManageApplicationPrivileges) conditionalPrivilege).getApplicationNames(),
            containsInAnyOrder("kibana", "logstash"));

        q = "{\"applications\": [{\"application\": \"myapp\", \"resources\": [\"*\"], \"privileges\": [\"login\" ]}] }";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertThat(rd.getName(), equalTo("test"));
        assertThat(rd.getClusterPrivileges(), emptyArray());
        assertThat(rd.getIndicesPrivileges(), emptyArray());
        assertThat(rd.getApplicationPrivileges().length, equalTo(1));
        assertThat(rd.getApplicationPrivileges()[0].getResources(), arrayContaining("*"));
        assertThat(rd.getApplicationPrivileges()[0].getPrivileges(), arrayContaining("login"));
        assertThat(rd.getApplicationPrivileges()[0].getApplication(), equalTo("myapp"));
        assertThat(rd.getConditionalClusterPrivileges(), Matchers.arrayWithSize(0));

        final String badJson
                = "{\"applications\":[{\"not_supported\": true, \"resources\": [\"*\"], \"privileges\": [\"my-app:login\" ]}] }";
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class,
                () -> RoleDescriptor.parse("test", new BytesArray(badJson), false, XContentType.JSON));
        assertThat(ex.getMessage(), containsString("not_supported"));
    }

    public void testSerializationForCurrentVersion() throws Exception {
        final Version version = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        logger.info("Testing serialization with version {}", version);
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(version);
        RoleDescriptor.IndicesPrivileges[] groups = new RoleDescriptor.IndicesPrivileges[] {
                RoleDescriptor.IndicesPrivileges.builder()
                        .indices("i1", "i2")
                        .privileges("read")
                        .grantedFields("body", "title")
                        .query("{\"query\": {\"match_all\": {}}}")
                        .build()
        };
        final RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges = {
            RoleDescriptor.ApplicationResourcePrivileges.builder()
                .application("my_app")
                .privileges("read", "write")
                .resources("*")
                .build()
        };
        final ConfigurableClusterPrivilege[] configurableClusterPrivileges = {
            new ConfigurableClusterPrivileges.ManageApplicationPrivileges(new LinkedHashSet<>(Arrays.asList("app01", "app02")))
        };

        Map<String, Object> metadata = randomBoolean() ? MetadataUtils.DEFAULT_RESERVED_METADATA : null;
        final RoleDescriptor descriptor = new RoleDescriptor("test", new String[]{"all", "none"}, groups, applicationPrivileges,
            configurableClusterPrivileges, new String[] { "sudo" }, metadata, null);
        descriptor.writeTo(output);
        final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin(Settings.EMPTY).getNamedWriteables());
        StreamInput streamInput = new NamedWriteableAwareStreamInput(ByteBufferStreamInput.wrap(BytesReference.toBytes(output.bytes())),
            registry);
        streamInput.setVersion(version);
        final RoleDescriptor serialized = new RoleDescriptor(streamInput);
        assertEquals(descriptor, serialized);
    }

    public void testParseEmptyQuery() throws Exception {
        String json = "{\"cluster\":[\"a\", \"b\"], \"run_as\": [\"m\", \"n\"], \"index\": [{\"names\": [\"idx1\",\"idx2\"], " +
                "\"privileges\": [\"p1\", \"p2\"], \"query\": \"\"}]}";
        RoleDescriptor rd = RoleDescriptor.parse("test", new BytesArray(json), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(1, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "idx1", "idx2" }, rd.getIndicesPrivileges()[0].getIndices());
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());
        assertNull(rd.getIndicesPrivileges()[0].getQuery());
    }

    public void testParseEmptyQueryUsingDeprecatedIndicesField() throws Exception {
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
        final RoleDescriptor descriptor = new RoleDescriptor("test", new String[] { "all" }, null, null, null, null,
                Collections.singletonMap("_unlicensed_feature", true), Collections.singletonMap("foo", "bar"));
        XContentBuilder b = jsonBuilder();
        descriptor.toXContent(b, ToXContent.EMPTY_PARAMS);
        RoleDescriptor parsed = RoleDescriptor.parse("test", BytesReference.bytes(b), false, XContentType.JSON);
        assertNotNull(parsed);
        assertEquals(1, parsed.getTransientMetadata().size());
        assertEquals(true, parsed.getTransientMetadata().get("enabled"));
    }
}
