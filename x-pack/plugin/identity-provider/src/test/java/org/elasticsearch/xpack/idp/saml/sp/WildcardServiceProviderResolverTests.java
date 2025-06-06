/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.saml.sp;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;
import org.junit.Before;
import org.opensaml.saml.saml2.core.NameID;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class WildcardServiceProviderResolverTests extends IdpSamlTestCase {

    private static final String SERVICES_JSON = """
        {
          "services": {
            "service1a": {
              "entity_id": "https://(?<service>\\\\w+)\\\\.example\\\\.com/",
              "acs": "https://(?<service>\\\\w+)\\\\.service\\\\.example\\\\.com/saml2/acs",
              "tokens": [ "service" ],
              "template": {
                "name": "{{service}} at example.com (A)",
                "privileges": {
                  "resource": "service1:example:{{service}}",
                  "roles": [ "sso:(.*)" ]
                },
                "attributes": {
                  "principal": "http://cloud.elastic.co/saml/principal",
                  "name": "http://cloud.elastic.co/saml/name",
                  "email": "http://cloud.elastic.co/saml/email",
                  "roles": "http://cloud.elastic.co/saml/roles"
                }
              }
            },
            "service1b": {
              "entity_id": "https://(?<service>\\\\w+)\\\\.example\\\\.com/",
              "acs": "https://services\\\\.example\\\\.com/(?<service>\\\\w+)/saml2/acs",
              "tokens": [ "service" ],
              "template": {
                "name": "{{service}} at example.com (B)",
                "privileges": {
                  "resource": "service1:example:{{service}}",
                  "roles": [ "sso:(.*)" ]
                },
                "attributes": {
                  "principal": "http://cloud.elastic.co/saml/principal",
                  "name": "http://cloud.elastic.co/saml/name",
                  "email": "http://cloud.elastic.co/saml/email",
                  "roles": "http://cloud.elastic.co/saml/roles"
                }
              }
            },
            "service2": {
              "entity_id": "https://service-(?<id>\\\\d+)\\\\.example\\\\.net/",
              "acs": "https://saml\\\\.example\\\\.net/(?<id>\\\\d+)/acs",
              "tokens": [ "id" ],
              "template": {
                "name": "{{id}} at example.net",
                "privileges": {
                  "resource": "service2:example:{{id}}",
                  "roles": [ "sso:(.*)" ]
                },
                "attributes": {
                  "principal": "http://cloud.elastic.co/saml/principal",
                  "name": "http://cloud.elastic.co/saml/name",
                  "email": "http://cloud.elastic.co/saml/email",
                  "roles": "http://cloud.elastic.co/saml/roles",
                  "extensions": [ "http://cloud.elastic.co/saml/department", "http://cloud.elastic.co/saml/avatar" ]
                }
              }
            }
          }
        }"""; // root object
    private WildcardServiceProviderResolver resolver;

    @Before
    public void setUpResolver() {
        final Settings settings = Settings.EMPTY;
        final ScriptService scriptService = new ScriptService(
            settings,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine(Settings.EMPTY)),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );
        final ServiceProviderDefaults samlDefaults = new ServiceProviderDefaults("elastic-cloud", NameID.TRANSIENT, Duration.ofMinutes(15));
        resolver = new WildcardServiceProviderResolver(settings, scriptService, new SamlServiceProviderFactory(samlDefaults));
    }

    public void testParsingOfServices() throws IOException {
        loadJsonServices();
        assertThat(resolver.services().keySet(), containsInAnyOrder("service1a", "service1b", "service2"));

        final WildcardServiceProvider service1a = resolver.services().get("service1a");
        assertThat(
            service1a.extractTokens("https://abcdef.example.com/", "https://abcdef.service.example.com/saml2/acs"),
            equalTo(
                Map.ofEntries(
                    Map.entry("service", "abcdef"),
                    Map.entry("entity_id", "https://abcdef.example.com/"),
                    Map.entry("acs", "https://abcdef.service.example.com/saml2/acs")
                )
            )
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> service1a.extractTokens("https://abcdef.example.com/", "https://different.service.example.com/saml2/acs")
        );
        assertThat(service1a.extractTokens("urn:foo:bar", "https://something.example.org/foo/bar"), nullValue());
        assertThat(service1a.extractTokens("https://xyzzy.example.com/", "https://services.example.com/xyzzy/saml2/acs"), nullValue());

        final WildcardServiceProvider service1b = resolver.services().get("service1b");
        assertThat(
            service1b.extractTokens("https://xyzzy.example.com/", "https://services.example.com/xyzzy/saml2/acs"),
            equalTo(
                Map.ofEntries(
                    Map.entry("service", "xyzzy"),
                    Map.entry("entity_id", "https://xyzzy.example.com/"),
                    Map.entry("acs", "https://services.example.com/xyzzy/saml2/acs")
                )
            )
        );
        assertThat(service1b.extractTokens("https://abcdef.example.com/", "https://abcdef.service.example.com/saml2/acs"), nullValue());
        expectThrows(
            IllegalArgumentException.class,
            () -> service1b.extractTokens("https://abcdef.example.com/", "https://services.example.com/xyzzy/saml2/acs")
        );
        assertThat(service1b.extractTokens("urn:foo:bar", "https://something.example.org/foo/bar"), nullValue());
    }

    public void testResolveServices() throws IOException {
        loadJsonServices();

        final SamlServiceProvider sp1 = resolver.resolve("https://abcdef.example.com/", "https://abcdef.service.example.com/saml2/acs");

        assertThat(sp1, notNullValue());
        assertThat(sp1.getEntityId(), equalTo("https://abcdef.example.com/"));
        assertThat(sp1.getAssertionConsumerService().toString(), equalTo("https://abcdef.service.example.com/saml2/acs"));
        assertThat(sp1.getName(), equalTo("abcdef at example.com (A)"));
        assertThat(sp1.getPrivileges().getResource(), equalTo("service1:example:abcdef"));
        assertThat(sp1.getAttributeNames().allowedExtensions, empty());

        final SamlServiceProvider sp2 = resolver.resolve("https://qwerty.example.com/", "https://qwerty.service.example.com/saml2/acs");
        assertThat(sp2, notNullValue());
        assertThat(sp2.getEntityId(), equalTo("https://qwerty.example.com/"));
        assertThat(sp2.getAssertionConsumerService().toString(), equalTo("https://qwerty.service.example.com/saml2/acs"));
        assertThat(sp2.getName(), equalTo("qwerty at example.com (A)"));
        assertThat(sp2.getPrivileges().getResource(), equalTo("service1:example:qwerty"));
        assertThat(sp2.getAttributeNames().allowedExtensions, empty());

        final SamlServiceProvider sp3 = resolver.resolve("https://xyzzy.example.com/", "https://services.example.com/xyzzy/saml2/acs");
        assertThat(sp3, notNullValue());
        assertThat(sp3.getEntityId(), equalTo("https://xyzzy.example.com/"));
        assertThat(sp3.getAssertionConsumerService().toString(), equalTo("https://services.example.com/xyzzy/saml2/acs"));
        assertThat(sp3.getName(), equalTo("xyzzy at example.com (B)"));
        assertThat(sp3.getPrivileges().getResource(), equalTo("service1:example:xyzzy"));
        assertThat(sp3.getAttributeNames().allowedExtensions, empty());

        final SamlServiceProvider sp4 = resolver.resolve("https://service-12345.example.net/", "https://saml.example.net/12345/acs");
        assertThat(sp4, notNullValue());
        assertThat(sp4.getEntityId(), equalTo("https://service-12345.example.net/"));
        assertThat(sp4.getAssertionConsumerService().toString(), equalTo("https://saml.example.net/12345/acs"));
        assertThat(sp4.getName(), equalTo("12345 at example.net"));
        assertThat(sp4.getPrivileges().getResource(), equalTo("service2:example:12345"));
        assertThat(
            sp4.getAttributeNames().allowedExtensions,
            containsInAnyOrder("http://cloud.elastic.co/saml/department", "http://cloud.elastic.co/saml/avatar")
        );

        expectThrows(
            IllegalArgumentException.class,
            () -> resolver.resolve("https://zbcdef.example.com/", "https://abcdef.service.example.com/saml2/acs")
        );
    }

    public void testCaching() throws IOException {
        loadJsonServices();

        final String serviceName = randomAlphaOfLengthBetween(4, 12);
        final String entityId = "https://" + serviceName + ".example.com/";
        final String acs = randomBoolean()
            ? "https://" + serviceName + ".service.example.com/saml2/acs"
            : "https://services.example.com/" + serviceName + "/saml2/acs";

        final SamlServiceProvider original = resolver.resolve(entityId, acs);
        for (int i = randomIntBetween(10, 20); i > 0; i--) {
            final SamlServiceProvider cached = resolver.resolve(entityId, acs);
            assertThat(cached, sameInstance(original));
        }
    }

    private void loadJsonServices() throws IOException {
        assertThat("Resolver has not been setup correctly", resolver, notNullValue());
        resolver.reload(createParser(XContentType.JSON.xContent(), SERVICES_JSON));
    }
}
