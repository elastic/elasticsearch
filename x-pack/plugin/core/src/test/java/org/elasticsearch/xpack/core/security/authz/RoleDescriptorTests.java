/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestMatchers;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.ApplicationResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
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

    public void testEqualsOnEmptyRoles() {
        RoleDescriptor nullRoleDescriptor = new RoleDescriptor(
            "null_role",
            randomFrom((String[]) null, new String[0]),
            randomFrom((RoleDescriptor.IndicesPrivileges[]) null, new RoleDescriptor.IndicesPrivileges[0]),
            randomFrom((ApplicationResourcePrivileges[]) null, new ApplicationResourcePrivileges[0]),
            randomFrom((ConfigurableClusterPrivilege[]) null, new ConfigurableClusterPrivilege[0]),
            randomFrom((String[]) null, new String[0]),
            randomFrom((Map<String, Object>) null, Map.of()),
            Map.of("transient", "meta", "is", "ignored")
        );
        assertTrue(nullRoleDescriptor.equals(new RoleDescriptor("null_role", null, null, null, null, null, null, null)));
    }

    public void testToString() {
        RoleDescriptor.IndicesPrivileges[] groups = new RoleDescriptor.IndicesPrivileges[] {
            RoleDescriptor.IndicesPrivileges.builder()
                .indices("i1", "i2")
                .privileges("read")
                .grantedFields("body", "title")
                .query("{\"match_all\": {}}")
                .build() };
        final ApplicationResourcePrivileges[] applicationPrivileges = {
            ApplicationResourcePrivileges.builder().application("my_app").privileges("read", "write").resources("*").build() };

        final ConfigurableClusterPrivilege[] configurableClusterPrivileges = new ConfigurableClusterPrivilege[] {
            new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(new LinkedHashSet<>(Arrays.asList("app*"))),
            new ConfigurableClusterPrivileges.ManageApplicationPrivileges(new LinkedHashSet<>(Arrays.asList("app01", "app02"))) };

        RoleDescriptor descriptor = new RoleDescriptor(
            "test",
            new String[] { "all", "none" },
            groups,
            applicationPrivileges,
            configurableClusterPrivileges,
            new String[] { "sudo" },
            Collections.emptyMap(),
            Collections.emptyMap()
        );

        assertThat(
            descriptor.toString(),
            is(
                "Role[name=test, cluster=[all,none]"
                    + ", global=[{APPLICATION:manage:applications=app01,app02},{PROFILE:write:applications=app*}]"
                    + ", indicesPrivileges=[IndicesPrivileges[indices=[i1,i2], allowRestrictedIndices=[false], privileges=[read]"
                    + ", field_security=[grant=[body,title], except=null], query={\"match_all\": {}}],]"
                    + ", applicationPrivileges=[ApplicationResourcePrivileges[application=my_app, privileges=[read,write], resources=[*]],]"
                    + ", runAs=[sudo], metadata=[{}]]"
            )
        );
    }

    public void testToXContentRoundtrip() throws Exception {
        final RoleDescriptor descriptor = randomRoleDescriptor();
        final XContentType xContentType = randomFrom(XContentType.values());
        final BytesReference xContentValue = toShuffledXContent(descriptor, xContentType, ToXContent.EMPTY_PARAMS, false);
        final RoleDescriptor parsed = RoleDescriptor.parse(descriptor.getName(), xContentValue, false, xContentType);
        assertThat(parsed, equalTo(descriptor));
    }

    public void testParse() throws Exception {
        String q = "{\"cluster\":[\"a\", \"b\"]}";
        RoleDescriptor rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(0, rd.getIndicesPrivileges().length);
        assertArrayEquals(Strings.EMPTY_ARRAY, rd.getRunAs());

        q = """
            {
              "cluster": [ "a", "b" ],
              "run_as": [ "m", "n" ]
            }""";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(0, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());

        q = """
            {
              "cluster": [ "a", "b" ],
              "run_as": [ "m", "n" ],
              "index": [
                {
                  "names": "idx1",
                  "privileges": [ "p1", "p2" ]
                },
                {
                  "names": "idx2",
                  "allow_restricted_indices": true,
                  "privileges": [ "p3" ],
                  "field_security": {
                    "grant": [ "f1", "f2" ]
                  }
                },
                {
                  "names": "idx2",
                  "allow_restricted_indices": false,
                  "privileges": [ "p3" ],
                  "field_security": {
                    "grant": [ "f1", "f2" ]
                  },
                  "query": {
                    "match_all": {}
                  }
                }
              ]
            }""";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(3, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());

        q = """
            {
              "cluster": [ "a", "b" ],
              "run_as": [ "m", "n" ],
              "index": [
                {
                  "names": [ "idx1", "idx2" ],
                  "privileges": [ "p1", "p2" ],
                  "allow_restricted_indices": true
                }
              ],
              "global": {
                "profile": {
                }
              }
            }""";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(1, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "idx1", "idx2" }, rd.getIndicesPrivileges()[0].getIndices());
        assertTrue(rd.getIndicesPrivileges()[0].allowRestrictedIndices());
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());
        assertNull(rd.getIndicesPrivileges()[0].getQuery());

        q = """
            {"cluster":["a", "b"], "metadata":{"foo":"bar"}}""";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(0, rd.getIndicesPrivileges().length);
        assertArrayEquals(Strings.EMPTY_ARRAY, rd.getRunAs());
        assertNotNull(rd.getMetadata());
        assertThat(rd.getMetadata().size(), is(1));
        assertThat(rd.getMetadata().get("foo"), is("bar"));

        q = """
            {
              "cluster": [ "a", "b" ],
              "run_as": [ "m", "n" ],
              "index": [
                {
                  "names": [ "idx1", "idx2" ],
                  "allow_restricted_indices": false,
                  "privileges": [ "p1", "p2" ]
                }
              ],
              "applications": [
                {
                  "resources": [ "object-123", "object-456" ],
                  "privileges": [ "read", "delete" ],
                  "application": "app1"
                },
                {
                  "resources": [ "*" ],
                  "privileges": [ "admin" ],
                  "application": "app2"
                }
              ],
              "global": {
                "application": {
                  "manage": {
                    "applications": [ "kibana", "logstash" ]
                  }
                },
                "profile": {
                }
              }
            }""";
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

        ConfigurableClusterPrivilege conditionalPrivilege = rd.getConditionalClusterPrivileges()[0];
        assertThat(conditionalPrivilege.getCategory(), equalTo(ConfigurableClusterPrivilege.Category.APPLICATION));
        assertThat(conditionalPrivilege, instanceOf(ConfigurableClusterPrivileges.ManageApplicationPrivileges.class));
        assertThat(
            ((ConfigurableClusterPrivileges.ManageApplicationPrivileges) conditionalPrivilege).getApplicationNames(),
            containsInAnyOrder("kibana", "logstash")
        );

        q = """
            {
              "cluster": [ "manage" ],
              "global": {
                "profile": {
                  "write": {
                    "applications": [ "", "kibana-*" ]
                  }
                },
                "application": {
                  "manage": {
                    "applications": [ "apm*", "kibana-1" ]
                  }
                }
              }
            }""";
        rd = RoleDescriptor.parse("testUpdateProfile", new BytesArray(q), false, XContentType.JSON);
        assertThat(rd.getName(), is("testUpdateProfile"));
        assertThat(rd.getClusterPrivileges(), arrayContaining("manage"));
        assertThat(rd.getIndicesPrivileges(), Matchers.emptyArray());
        assertThat(rd.getRunAs(), Matchers.emptyArray());
        assertThat(rd.getApplicationPrivileges(), Matchers.emptyArray());
        assertThat(rd.getConditionalClusterPrivileges(), Matchers.arrayWithSize(2));

        conditionalPrivilege = rd.getConditionalClusterPrivileges()[0];
        assertThat(conditionalPrivilege.getCategory(), equalTo(ConfigurableClusterPrivilege.Category.APPLICATION));
        assertThat(conditionalPrivilege, instanceOf(ConfigurableClusterPrivileges.ManageApplicationPrivileges.class));
        assertThat(
            ((ConfigurableClusterPrivileges.ManageApplicationPrivileges) conditionalPrivilege).getApplicationNames(),
            containsInAnyOrder("apm*", "kibana-1")
        );
        conditionalPrivilege = rd.getConditionalClusterPrivileges()[1];
        assertThat(conditionalPrivilege.getCategory(), equalTo(ConfigurableClusterPrivilege.Category.PROFILE));
        assertThat(conditionalPrivilege, instanceOf(ConfigurableClusterPrivileges.WriteProfileDataPrivileges.class));
        assertThat(
            ((ConfigurableClusterPrivileges.WriteProfileDataPrivileges) conditionalPrivilege).getApplicationNames(),
            containsInAnyOrder("", "kibana-*")
        );

        q = """
            {"applications": [{"application": "myapp", "resources": ["*"], "privileges": ["login" ]}] }""";
        rd = RoleDescriptor.parse("test", new BytesArray(q), false, XContentType.JSON);
        assertThat(rd.getName(), equalTo("test"));
        assertThat(rd.getClusterPrivileges(), emptyArray());
        assertThat(rd.getIndicesPrivileges(), emptyArray());
        assertThat(rd.getApplicationPrivileges().length, equalTo(1));
        assertThat(rd.getApplicationPrivileges()[0].getResources(), arrayContaining("*"));
        assertThat(rd.getApplicationPrivileges()[0].getPrivileges(), arrayContaining("login"));
        assertThat(rd.getApplicationPrivileges()[0].getApplication(), equalTo("myapp"));
        assertThat(rd.getConditionalClusterPrivileges(), Matchers.arrayWithSize(0));

        final String badJson = """
            {"applications":[{"not_supported": true, "resources": ["*"], "privileges": ["my-app:login" ]}] }""";
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> RoleDescriptor.parse("test", new BytesArray(badJson), false, XContentType.JSON)
        );
        assertThat(ex.getMessage(), containsString("not_supported"));
    }

    public void testSerializationForCurrentVersion() throws Exception {
        final Version version = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
        logger.info("Testing serialization with version {}", version);
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(version);

        final RoleDescriptor descriptor = randomRoleDescriptor();
        descriptor.writeTo(output);
        final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin().getNamedWriteables());
        StreamInput streamInput = new NamedWriteableAwareStreamInput(
            ByteBufferStreamInput.wrap(BytesReference.toBytes(output.bytes())),
            registry
        );
        streamInput.setVersion(version);
        final RoleDescriptor serialized = new RoleDescriptor(streamInput);
        assertThat(serialized, equalTo(descriptor));
    }

    public void testParseEmptyQuery() throws Exception {
        String json = """
            {
                "cluster": [ "a", "b" ],
                "run_as": [ "m", "n" ],
                "index": [
                  {
                    "names": [ "idx1", "idx2" ],
                    "privileges": [ "p1", "p2" ],
                    "query": ""
                  }
                ]
              }""";
        RoleDescriptor rd = RoleDescriptor.parse("test", new BytesArray(json), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(1, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "idx1", "idx2" }, rd.getIndicesPrivileges()[0].getIndices());
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());
        assertNull(rd.getIndicesPrivileges()[0].getQuery());
    }

    public void testParseNullQuery() throws Exception {
        String json = """
            {
              "cluster": [ "a", "b" ],
              "run_as": [ "m", "n" ],
              "index": [
                {
                  "names": [ "idx1", "idx2" ],
                  "privileges": [ "p1", "p2" ],
                  "query": null
                }
              ]
            }""";
        RoleDescriptor rd = RoleDescriptor.parse("test", new BytesArray(json), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(1, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "idx1", "idx2" }, rd.getIndicesPrivileges()[0].getIndices());
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());
        assertNull(rd.getIndicesPrivileges()[0].getQuery());
    }

    public void testParseEmptyQueryUsingDeprecatedIndicesField() throws Exception {
        String json = """
            {
              "cluster": [ "a", "b" ],
              "run_as": [ "m", "n" ],
              "indices": [
                {
                  "names": [ "idx1", "idx2" ],
                  "privileges": [ "p1", "p2" ],
                  "query": ""
                }
              ]
            }""";
        RoleDescriptor rd = RoleDescriptor.parse("test", new BytesArray(json), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertArrayEquals(new String[] { "a", "b" }, rd.getClusterPrivileges());
        assertEquals(1, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "idx1", "idx2" }, rd.getIndicesPrivileges()[0].getIndices());
        assertArrayEquals(new String[] { "m", "n" }, rd.getRunAs());
        assertNull(rd.getIndicesPrivileges()[0].getQuery());
    }

    public void testParseIgnoresTransientMetadata() throws Exception {
        final RoleDescriptor descriptor = new RoleDescriptor(
            "test",
            new String[] { "all" },
            null,
            null,
            null,
            null,
            Collections.singletonMap("_unlicensed_feature", true),
            Collections.singletonMap("foo", "bar")
        );
        XContentBuilder b = jsonBuilder();
        descriptor.toXContent(b, ToXContent.EMPTY_PARAMS);
        RoleDescriptor parsed = RoleDescriptor.parse("test", BytesReference.bytes(b), false, XContentType.JSON);
        assertNotNull(parsed);
        assertEquals(1, parsed.getTransientMetadata().size());
        assertEquals(true, parsed.getTransientMetadata().get("enabled"));
    }

    public void testParseIndicesPrivilegesSucceedsWhenExceptFieldsIsSubsetOfGrantedFields() throws IOException {
        final boolean grantAll = randomBoolean();
        final String grant = grantAll ? "\"*\"" : "\"f1\",\"f2\"";
        final String except = grantAll ? "\"_fx\",\"f8\"" : "\"f1\"";

        final String json = """
            {
              "indices": [
                {
                  "names": [ "idx1", "idx2" ],
                  "privileges": [ "p1", "p2" ],
                  "field_security": {
                    "grant": [ %s ],
                    "except": [ %s ]
                  }
                }
              ]
            }""".formatted(grant, except);
        final RoleDescriptor rd = RoleDescriptor.parse("test", new BytesArray(json), false, XContentType.JSON);
        assertEquals("test", rd.getName());
        assertEquals(1, rd.getIndicesPrivileges().length);
        assertArrayEquals(new String[] { "idx1", "idx2" }, rd.getIndicesPrivileges()[0].getIndices());
        assertArrayEquals((grantAll) ? new String[] { "*" } : new String[] { "f1", "f2" }, rd.getIndicesPrivileges()[0].getGrantedFields());
        assertArrayEquals(
            (grantAll) ? new String[] { "_fx", "f8" } : new String[] { "f1" },
            rd.getIndicesPrivileges()[0].getDeniedFields()
        );
    }

    public void testParseIndicesPrivilegesFailsWhenExceptFieldsAreNotSubsetOfGrantedFields() {
        final String json = """
            {
              "indices": [
                {
                  "names": [ "idx1", "idx2" ],
                  "privileges": [ "p1", "p2" ],
                  "field_security": {
                    "grant": [ "f1", "f2" ],
                    "except": [ "f3" ]
                  }
                }
              ]
            }""";
        final ElasticsearchParseException epe = expectThrows(
            ElasticsearchParseException.class,
            () -> RoleDescriptor.parse("test", new BytesArray(json), false, XContentType.JSON)
        );
        assertThat(epe, TestMatchers.throwableWithMessage(containsString("must be a subset of the granted fields ")));
        assertThat(epe, TestMatchers.throwableWithMessage(containsString("f1")));
        assertThat(epe, TestMatchers.throwableWithMessage(containsString("f2")));
        assertThat(epe, TestMatchers.throwableWithMessage(containsString("f3")));
    }

    public void testGlobalPrivilegesOrdering() throws IOException {
        final String roleName = randomAlphaOfLengthBetween(3, 30);
        final String[] applicationNames = generateRandomStringArray(3, randomIntBetween(0, 3), false, true);
        final String[] profileNames = generateRandomStringArray(3, randomIntBetween(0, 3), false, true);
        ConfigurableClusterPrivilege[] configurableClusterPrivileges = new ConfigurableClusterPrivilege[] {
            new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(Sets.newHashSet(profileNames)),
            new ConfigurableClusterPrivileges.ManageApplicationPrivileges(Sets.newHashSet(applicationNames)) };
        RoleDescriptor role1 = new RoleDescriptor(
            roleName,
            new String[0],
            new RoleDescriptor.IndicesPrivileges[0],
            new RoleDescriptor.ApplicationResourcePrivileges[0],
            configurableClusterPrivileges,
            new String[0],
            Map.of(),
            Map.of()
        );
        // swap
        var temp = configurableClusterPrivileges[0];
        configurableClusterPrivileges[0] = configurableClusterPrivileges[1];
        configurableClusterPrivileges[1] = temp;
        RoleDescriptor role2 = new RoleDescriptor(
            roleName,
            new String[0],
            new RoleDescriptor.IndicesPrivileges[0],
            new RoleDescriptor.ApplicationResourcePrivileges[0],
            configurableClusterPrivileges,
            new String[0],
            Map.of(),
            Map.of()
        );
        assertThat(role2, is(role1));
        StringBuilder applicationNamesString = new StringBuilder();
        for (int i = 0; i < applicationNames.length; i++) {
            if (i > 0) {
                applicationNamesString.append(", ");
            }
            applicationNamesString.append("\"" + applicationNames[i] + "\"");
        }
        StringBuilder profileNamesString = new StringBuilder();
        for (int i = 0; i < profileNames.length; i++) {
            if (i > 0) {
                profileNamesString.append(", ");
            }
            profileNamesString.append("\"" + profileNames[i] + "\"");
        }
        String json = """
            {
              "global": {
                "profile": {
                  "write": {
                    "applications": [ %s ]
                  }
                },
                "application": {
                  "manage": {
                    "applications": [ %s ]
                  }
                }
              }
            }""".formatted(profileNamesString, applicationNamesString);
        RoleDescriptor role3 = RoleDescriptor.parse(roleName, new BytesArray(json), false, XContentType.JSON);
        assertThat(role3, is(role1));
        json = """
            {
              "global": {
                "application": {
                  "manage": {
                    "applications": [ %s ]
                  }
                },
                "profile": {
                  "write": {
                    "applications": [ %s ]
                  }
                }
              }
            }""".formatted(applicationNamesString, profileNamesString);
        RoleDescriptor role4 = RoleDescriptor.parse(roleName, new BytesArray(json), false, XContentType.JSON);
        assertThat(role4, is(role1));
    }

    public void testIsEmpty() {
        assertTrue(new RoleDescriptor(randomAlphaOfLengthBetween(1, 10), null, null, null, null, null, null, null).isEmpty());

        assertTrue(
            new RoleDescriptor(
                randomAlphaOfLengthBetween(1, 10),
                new String[0],
                new RoleDescriptor.IndicesPrivileges[0],
                new ApplicationResourcePrivileges[0],
                new ConfigurableClusterPrivilege[0],
                new String[0],
                new HashMap<>(),
                new HashMap<>()
            ).isEmpty()
        );

        final List<Boolean> booleans = Arrays.asList(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );

        final RoleDescriptor roleDescriptor = new RoleDescriptor(
            randomAlphaOfLengthBetween(1, 10),
            booleans.get(0) ? new String[0] : new String[] { "foo" },
            booleans.get(1)
                ? new RoleDescriptor.IndicesPrivileges[0]
                : new RoleDescriptor.IndicesPrivileges[] {
                    RoleDescriptor.IndicesPrivileges.builder().indices("idx").privileges("foo").build() },
            booleans.get(2)
                ? new ApplicationResourcePrivileges[0]
                : new ApplicationResourcePrivileges[] {
                    ApplicationResourcePrivileges.builder().application("app").privileges("foo").resources("res").build() },
            booleans.get(3)
                ? new ConfigurableClusterPrivilege[0]
                : new ConfigurableClusterPrivilege[] {
                    randomBoolean()
                        ? new ConfigurableClusterPrivileges.ManageApplicationPrivileges(Collections.singleton("foo"))
                        : new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(Collections.singleton("bar")) },
            booleans.get(4) ? new String[0] : new String[] { "foo" },
            booleans.get(5) ? new HashMap<>() : Collections.singletonMap("foo", "bar"),
            Collections.singletonMap("foo", "bar")
        );

        if (booleans.stream().anyMatch(e -> e.equals(false))) {
            assertFalse(roleDescriptor.isEmpty());
        } else {
            assertTrue(roleDescriptor.isEmpty());
        }
    }

    public static List<RoleDescriptor> randomUniquelyNamedRoleDescriptors(int minSize, int maxSize) {
        return randomValueOtherThanMany(
            roleDescriptors -> roleDescriptors.stream().map(RoleDescriptor::getName).distinct().count() != roleDescriptors.size(),
            () -> randomList(minSize, maxSize, () -> randomRoleDescriptor(false))
        );
    }

    public static RoleDescriptor randomRoleDescriptor() {
        return randomRoleDescriptor(true);
    }

    public static RoleDescriptor randomRoleDescriptor(boolean allowReservedMetadata) {
        final RoleDescriptor.IndicesPrivileges[] indexPrivileges = new RoleDescriptor.IndicesPrivileges[randomIntBetween(0, 3)];
        for (int i = 0; i < indexPrivileges.length; i++) {
            final RoleDescriptor.IndicesPrivileges.Builder builder = RoleDescriptor.IndicesPrivileges.builder()
                .privileges(randomSubsetOf(randomIntBetween(1, 4), IndexPrivilege.names()))
                .indices(generateRandomStringArray(5, randomIntBetween(3, 9), false, false))
                .allowRestrictedIndices(randomBoolean());
            if (randomBoolean()) {
                builder.query(
                    randomBoolean()
                        ? "{ \"term\": { \"" + randomAlphaOfLengthBetween(3, 24) + "\" : \"" + randomAlphaOfLengthBetween(3, 24) + "\" }"
                        : "{ \"match_all\": {} }"
                );
            }
            if (randomBoolean()) {
                if (randomBoolean()) {
                    builder.grantedFields("*");
                    builder.deniedFields(generateRandomStringArray(4, randomIntBetween(4, 9), false, false));
                } else {
                    builder.grantedFields(generateRandomStringArray(4, randomIntBetween(4, 9), false, false));
                }
            }
            indexPrivileges[i] = builder.build();
        }
        final ApplicationResourcePrivileges[] applicationPrivileges = new ApplicationResourcePrivileges[randomIntBetween(0, 2)];
        for (int i = 0; i < applicationPrivileges.length; i++) {
            final ApplicationResourcePrivileges.Builder builder = ApplicationResourcePrivileges.builder();
            builder.application(randomAlphaOfLengthBetween(5, 12) + (randomBoolean() ? "*" : ""));
            if (randomBoolean()) {
                builder.privileges("*");
            } else {
                builder.privileges(generateRandomStringArray(6, randomIntBetween(4, 8), false, false));
            }
            if (randomBoolean()) {
                builder.resources("*");
            } else {
                builder.resources(generateRandomStringArray(6, randomIntBetween(4, 8), false, false));
            }
            applicationPrivileges[i] = builder.build();
        }
        final ConfigurableClusterPrivilege[] configurableClusterPrivileges = switch (randomIntBetween(0, 4)) {
            case 0 -> new ConfigurableClusterPrivilege[0];
            case 1 -> new ConfigurableClusterPrivilege[] {
                new ConfigurableClusterPrivileges.ManageApplicationPrivileges(
                    Sets.newHashSet(generateRandomStringArray(3, randomIntBetween(4, 12), false, false))
                ) };
            case 2 -> new ConfigurableClusterPrivilege[] {
                new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(
                    Sets.newHashSet(generateRandomStringArray(3, randomIntBetween(4, 12), false, false))
                ) };
            case 3 -> new ConfigurableClusterPrivilege[] {
                new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(
                    Sets.newHashSet(generateRandomStringArray(3, randomIntBetween(4, 12), false, false))
                ),
                new ConfigurableClusterPrivileges.ManageApplicationPrivileges(
                    Sets.newHashSet(generateRandomStringArray(3, randomIntBetween(4, 12), false, false))
                ) };
            case 4 -> new ConfigurableClusterPrivilege[] {
                new ConfigurableClusterPrivileges.ManageApplicationPrivileges(
                    Sets.newHashSet(generateRandomStringArray(3, randomIntBetween(4, 12), false, false))
                ),
                new ConfigurableClusterPrivileges.WriteProfileDataPrivileges(
                    Sets.newHashSet(generateRandomStringArray(3, randomIntBetween(4, 12), false, false))
                ) };
            default -> throw new IllegalStateException("Unexpected value");
        };
        final Map<String, Object> metadata = new HashMap<>();
        while (randomBoolean()) {
            String key = randomAlphaOfLengthBetween(4, 12);
            if (allowReservedMetadata && randomBoolean()) {
                key = MetadataUtils.RESERVED_PREFIX + key;
            }
            final Object value = randomBoolean() ? randomInt() : randomAlphaOfLengthBetween(3, 50);
            metadata.put(key, value);
        }

        return new RoleDescriptor(
            randomAlphaOfLengthBetween(3, 90),
            randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
            indexPrivileges,
            applicationPrivileges,
            configurableClusterPrivileges,
            generateRandomStringArray(5, randomIntBetween(2, 8), false, true),
            metadata,
            Map.of()
        );
    }
}
