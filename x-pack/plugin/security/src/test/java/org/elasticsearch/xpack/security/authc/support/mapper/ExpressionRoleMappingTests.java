/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.support.mapper;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AllExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.AnyExpression;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.FieldExpression;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.notNullValue;

public class ExpressionRoleMappingTests extends ESTestCase {

    private RealmConfig realm;

    @Before
    public void setupMapping() throws Exception {
        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("ldap", "ldap1");
        realm = new RealmConfig(
            realmIdentifier,
            Settings.builder().put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
            Mockito.mock(Environment.class),
            new ThreadContext(Settings.EMPTY)
        );
    }

    public void testValidExpressionWithFixedRoleNames() throws Exception {
        String json = """
            {
              "roles": [ "kibana_user", "sales" ],
              "enabled": true,
              "rules": {
                "all": [
                  {
                    "field": {
                      "dn": "*,ou=sales,dc=example,dc=com"
                    }
                  },
                  {
                    "except": {
                      "field": {
                        "metadata.active": false
                      }
                    }
                  }
                ]
              }
            }""";
        ExpressionRoleMapping mapping = parse(json, "ldap_sales");
        assertThat(mapping.getRoles(), Matchers.containsInAnyOrder("kibana_user", "sales"));
        assertThat(mapping.getExpression(), instanceOf(AllExpression.class));

        final UserRoleMapper.UserData user1a = new UserRoleMapper.UserData(
            "john.smith",
            "cn=john.smith,ou=sales,dc=example,dc=com",
            List.of(),
            Map.of("active", true),
            realm
        );
        final UserRoleMapper.UserData user1b = new UserRoleMapper.UserData(
            user1a.getUsername(),
            user1a.getDn().toUpperCase(Locale.US),
            user1a.getGroups(),
            user1a.getMetadata(),
            user1a.getRealm()
        );
        final UserRoleMapper.UserData user1c = new UserRoleMapper.UserData(
            user1a.getUsername(),
            user1a.getDn().replaceAll(",", ", "),
            user1a.getGroups(),
            user1a.getMetadata(),
            user1a.getRealm()
        );
        final UserRoleMapper.UserData user1d = new UserRoleMapper.UserData(
            user1a.getUsername(),
            user1a.getDn().replaceAll("dc=", "DC="),
            user1a.getGroups(),
            user1a.getMetadata(),
            user1a.getRealm()
        );
        final UserRoleMapper.UserData user2 = new UserRoleMapper.UserData(
            "jamie.perez",
            "cn=jamie.perez,ou=sales,dc=example,dc=com",
            List.of(),
            Map.of("active", false),
            realm
        );

        final UserRoleMapper.UserData user3 = new UserRoleMapper.UserData(
            "simone.ng",
            "cn=simone.ng,ou=finance,dc=example,dc=com",
            List.of(),
            Map.of("active", true),
            realm
        );

        final UserRoleMapper.UserData user4 = new UserRoleMapper.UserData("peter.null", null, List.of(), Map.of("active", true), realm);

        assertThat(mapping.getExpression().match(user1a.asModel()), equalTo(true));
        assertThat(mapping.getExpression().match(user1b.asModel()), equalTo(true));
        assertThat(mapping.getExpression().match(user1c.asModel()), equalTo(true));
        assertThat(mapping.getExpression().match(user1d.asModel()), equalTo(true));
        assertThat(mapping.getExpression().match(user2.asModel()), equalTo(false)); // metadata.active == false
        assertThat(mapping.getExpression().match(user3.asModel()), equalTo(false)); // dn != ou=sales,dc=example,dc=com
        assertThat(mapping.getExpression().match(user4.asModel()), equalTo(false)); // dn == null

        // expression without dn
        json = """
            {
              "roles": [ "superuser", "system_admin", "admin" ],
              "enabled": true,
              "rules": {
                "any": [
                  {
                    "field": {
                      "username": "tony.stark"
                    }
                  },
                  {
                    "field": {
                      "groups": "cn=admins,dc=stark-enterprises,dc=com"
                    }
                  }
                ]
              }
            }""";
        mapping = parse(json, "stark_admin");
        assertThat(mapping.getRoles(), Matchers.containsInAnyOrder("superuser", "system_admin", "admin"));
        assertThat(mapping.getExpression(), instanceOf(AnyExpression.class));

        final UserRoleMapper.UserData userTony = new UserRoleMapper.UserData(
            "tony.stark",
            null,
            List.of("Audi R8 owners"),
            Map.of("boss", true),
            realm
        );
        final UserRoleMapper.UserData userPepper = new UserRoleMapper.UserData(
            "pepper.potts",
            null,
            List.of("marvel", "cn=admins,dc=stark-enterprises,dc=com"),
            Map.of(),
            realm
        );
        final UserRoleMapper.UserData userMax = new UserRoleMapper.UserData(
            "max.rockatansky",
            null,
            List.of("bronze"),
            Map.of("mad", true),
            realm
        );
        assertThat(mapping.getExpression().match(userTony.asModel()), equalTo(true));
        assertThat(mapping.getExpression().match(userPepper.asModel()), equalTo(true));
        assertThat(mapping.getExpression().match(userMax.asModel()), equalTo(false));
    }

    public void testParseValidJsonWithTemplatedRoleNames() throws Exception {
        String json = """
            {
              "role_templates": [
                {
                  "template": {
                    "source": "kibana_user"
                  }
                },
                {
                  "template": {
                    "source": "sales"
                  }
                },
                {
                  "template": {
                    "source": "_user_{{username}}"
                  },
                  "format": "string"
                }
              ],
              "enabled": true,
              "rules": {
                "all": [
                  {
                    "field": {
                      "dn": "*,ou=sales,dc=example,dc=com"
                    }
                  },
                  {
                    "except": {
                      "field": {
                        "metadata.active": false
                      }
                    }
                  }
                ]
              }
            }""";
        final ExpressionRoleMapping mapping = parse(json, "ldap_sales");
        assertThat(mapping.getRoleTemplates(), iterableWithSize(3));
        assertThat(mapping.getRoleTemplates().get(0).getTemplate().utf8ToString(), equalTo("{\"source\":\"kibana_user\"}"));
        assertThat(mapping.getRoleTemplates().get(0).getFormat(), equalTo(TemplateRoleName.Format.STRING));
        assertThat(mapping.getRoleTemplates().get(1).getTemplate().utf8ToString(), equalTo("{\"source\":\"sales\"}"));
        assertThat(mapping.getRoleTemplates().get(1).getFormat(), equalTo(TemplateRoleName.Format.STRING));
        assertThat(mapping.getRoleTemplates().get(2).getTemplate().utf8ToString(), equalTo("{\"source\":\"_user_{{username}}\"}"));
        assertThat(mapping.getRoleTemplates().get(2).getFormat(), equalTo(TemplateRoleName.Format.STRING));
    }

    public void testParsingFailsIfRulesAreMissing() throws Exception {
        String json = """
            {
              "roles": [ "kibana_user", "sales" ],
              "enabled": true
            }""";
        ParsingException ex = expectThrows(ParsingException.class, () -> parse(json, "bad_json"));
        assertThat(ex.getMessage(), containsString("rules"));
    }

    public void testParsingFailsIfRolesMissing() throws Exception {
        String json = """
            {
              "enabled": true,
              "rules": {
                "field": {
                  "dn": "*,ou=sales,dc=example,dc=com"
                }
              }
            }""";
        ParsingException ex = expectThrows(ParsingException.class, () -> parse(json, "bad_json"));
        assertThat(ex.getMessage(), containsString("role"));
    }

    public void testParsingFailsIfThereAreUnrecognisedFields() throws Exception {
        String json = """
            {
              "disabled": false,
              "roles": [ "kibana_user", "sales" ],
              "rules": {
                "field": {
                  "dn": "*,ou=sales,dc=example,dc=com"
                }
              }
            }""";
        ParsingException ex = expectThrows(ParsingException.class, () -> parse(json, "bad_json"));
        assertThat(ex.getMessage(), containsString("disabled"));
    }

    public void testParsingIgnoresTypeFields() throws Exception {
        String json = """
            {
              "enabled": true,
              "roles": [ "kibana_user", "sales" ],
              "rules": {
                "field": {
                  "dn": "*,ou=sales,dc=example,dc=com"
                }
              },
              "doc_type": "role-mapping",
              "type": "doc"
            }""";
        final ExpressionRoleMapping mapping = parse(json, "from_index", true);
        assertThat(mapping.isEnabled(), equalTo(true));
        assertThat(mapping.getRoles(), Matchers.containsInAnyOrder("kibana_user", "sales"));
    }

    public void testParsingOfBothRoleNamesAndTemplates() throws Exception {
        String json = """
            {
              "enabled": true,
              "roles": [ "kibana_user", "sales" ],
              "role_templates": [ { "template": "{ \\"source\\":\\"_user_{{username}}\\" }", "format": "string" } ],
              "rules": {
                "field": {
                  "dn": "*,ou=sales,dc=example,dc=com"
                }
              }
            }""";

        // This is rejected when validating a request, but is valid when parsing the mapping
        final ExpressionRoleMapping mapping = parse(json, "from_api", false);
        assertThat(mapping.getRoles(), iterableWithSize(2));
        assertThat(mapping.getRoleTemplates(), iterableWithSize(1));
    }

    public void testToXContentWithRoleNames() throws Exception {
        String source = """
            {
              "roles": [ "kibana_user", "sales" ],
              "enabled": true,
              "rules": {
                "field": {
                  "realm.name": "saml1"
                }
              }
            }""";
        final ExpressionRoleMapping mapping = parse(source, getTestName());
        assertThat(mapping.getRoles(), iterableWithSize(2));

        final String xcontent = Strings.toString(mapping);
        assertThat(xcontent, equalTo("""
            {"enabled":true,"roles":["kibana_user","sales"],"rules":{"field":{"realm.name":"saml1"}},"metadata":{}}"""));
    }

    public void testToXContentWithTemplates() throws Exception {
        String source = """
            {
              "metadata": {
                "answer": 42
              },
              "role_templates": [
                {
                  "template": {
                    "source": "_user_{{username}}"
                  },
                  "format": "string"
                },
                {
                  "template": {
                    "source": "{{#tojson}}groups{{/tojson}}"
                  },
                  "format": "json"
                }
              ],
              "enabled": false,
              "rules": {
                "field": {
                  "realm.name": "saml1"
                }
              }
            }""";
        final ExpressionRoleMapping mapping = parse(source, getTestName());
        assertThat(mapping.getRoleTemplates(), iterableWithSize(2));

        final String xcontent = Strings.toString(mapping.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS, true));
        assertThat(xcontent, equalTo(XContentHelper.stripWhitespace("""
            {
              "enabled": false,
              "role_templates": [
                {
                  "template": "{\\"source\\":\\"_user_{{username}}\\"}",
                  "format": "string"
                },
                {
                  "template": "{\\"source\\":\\"{{#tojson}}groups{{/tojson}}\\"}",
                  "format": "json"
                }
              ],
              "rules": {
                "field": {
                  "realm.name": "saml1"
                }
              },
              "metadata": {
                "answer": 42
              },
              "doc_type": "role-mapping"
            }""")));

        final ExpressionRoleMapping parsed = parse(xcontent, getTestName(), true);
        assertThat(parsed.getRoles(), iterableWithSize(0));
        assertThat(parsed.getRoleTemplates(), iterableWithSize(2));
        assertThat(parsed.getMetadata(), Matchers.hasKey("answer"));
    }

    public void testSerialization() throws Exception {
        final ExpressionRoleMapping original = randomRoleMapping(true);

        final Version version = VersionUtils.randomVersionBetween(random(), Version.V_7_2_0, null);
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(version);
        original.writeTo(output);

        final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin().getNamedWriteables());
        StreamInput streamInput = new NamedWriteableAwareStreamInput(
            ByteBufferStreamInput.wrap(BytesReference.toBytes(output.bytes())),
            registry
        );
        streamInput.setVersion(version);
        final ExpressionRoleMapping serialized = new ExpressionRoleMapping(streamInput);
        assertEquals(original, serialized);
    }

    public void testSerializationPreV71() throws Exception {
        final ExpressionRoleMapping original = randomRoleMapping(false);

        final Version version = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_0_1);
        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(version);
        original.writeTo(output);

        final NamedWriteableRegistry registry = new NamedWriteableRegistry(new XPackClientPlugin().getNamedWriteables());
        StreamInput streamInput = new NamedWriteableAwareStreamInput(
            ByteBufferStreamInput.wrap(BytesReference.toBytes(output.bytes())),
            registry
        );
        streamInput.setVersion(version);
        final ExpressionRoleMapping serialized = new ExpressionRoleMapping(streamInput);
        assertEquals(original, serialized);
    }

    private ExpressionRoleMapping parse(String json, String name) throws IOException {
        return parse(json, name, false);
    }

    private ExpressionRoleMapping parse(String json, String name, boolean fromIndex) throws IOException {
        final NamedXContentRegistry registry = NamedXContentRegistry.EMPTY;
        final XContentParser parser = XContentType.JSON.xContent()
            .createParser(XContentParserConfiguration.EMPTY.withRegistry(registry), json);
        final ExpressionRoleMapping mapping = ExpressionRoleMapping.parse(name, parser);
        assertThat(mapping, notNullValue());
        assertThat(mapping.getName(), equalTo(name));
        return mapping;
    }

    private ExpressionRoleMapping randomRoleMapping(boolean acceptRoleTemplates) {
        final boolean useTemplate = acceptRoleTemplates && randomBoolean();
        final List<String> roles;
        final List<TemplateRoleName> templates;
        if (useTemplate) {
            roles = Collections.emptyList();
            templates = Arrays.asList(
                randomArray(
                    1,
                    5,
                    TemplateRoleName[]::new,
                    () -> new TemplateRoleName(
                        new BytesArray(randomAlphaOfLengthBetween(10, 25)),
                        randomFrom(TemplateRoleName.Format.values())
                    )
                )
            );
        } else {
            roles = Arrays.asList(randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(4, 12)));
            templates = Collections.emptyList();
        }
        return new ExpressionRoleMapping(
            randomAlphaOfLengthBetween(3, 8),
            new FieldExpression(
                randomAlphaOfLengthBetween(4, 12),
                Collections.singletonList(new FieldExpression.FieldValue(randomInt(99)))
            ),
            roles,
            templates,
            Collections.singletonMap(randomAlphaOfLengthBetween(3, 12), randomIntBetween(30, 90)),
            true
        );
    }

}
