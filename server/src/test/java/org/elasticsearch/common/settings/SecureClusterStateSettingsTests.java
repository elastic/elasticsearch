/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class SecureClusterStateSettingsTests extends ESTestCase {

    private static final String FLAT_SECRETS = Strings.format("""
        {
          "file_secrets": {
            "foo": "%s"
          },
          "string_secrets": {
            "goo": "baz"
          }
        }
        """, Base64.getEncoder().encodeToString("bar".getBytes(StandardCharsets.UTF_8)));

    private static final String NESTED_SECRETS = Strings.format("""
        {
          "file_secrets": {
            "foo": {
              "one": {
                "a": "%s"
              }
            }
          },
          "string_secrets": {
            "goo": {
              "one": {
                "a": "oneA",
                "b": "oneB"
              },
              "two": "two"
            }
          }
        }
        """, Base64.getEncoder().encodeToString("bar".getBytes(StandardCharsets.UTF_8)));

    private final SecureClusterStateSettings secureClusterStateSettings = getSecureClusterStateSettings(FLAT_SECRETS);

    public void testFromXContent() throws IOException {
        assertThat(secureClusterStateSettings.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(secureClusterStateSettings.getString("foo").toString(), is("bar"));
        assertThat(secureClusterStateSettings.getString("goo").toString(), is("baz"));
        assertThat(readString(secureClusterStateSettings.getFile("foo")), is("bar"));
        assertThat(readString(secureClusterStateSettings.getFile("goo")), is("baz"));

        // throw on duplicate key
        var e = assertThrows(XContentParseException.class, () -> getSecureClusterStateSettings(FLAT_SECRETS.replace("goo", "foo")));
        assertThat(rootCause(e), hasToString(containsString("Some secrets were defined as both string and file secrets: [foo]")));

        // throw on unexpected JSON token (null)
        e = assertThrows(XContentParseException.class, () -> getSecureClusterStateSettings(FLAT_SECRETS.replace("\"baz\"", "null")));
        assertThat(rootCause(e), hasToString(containsString("unexpected token [VALUE_NULL]")));

        // throw on unexpected JSON token (number)
        e = assertThrows(XContentParseException.class, () -> getSecureClusterStateSettings(FLAT_SECRETS.replace("\"baz\"", "4")));
        assertThat(rootCause(e), hasToString(containsString("unexpected token [VALUE_NUMBER]")));

        // throw on unexpected JSON token (array)
        e = assertThrows(XContentParseException.class, () -> getSecureClusterStateSettings(FLAT_SECRETS.replace("\"baz\"", "[]")));
        assertThat(rootCause(e), hasToString(containsString("unexpected token [START_ARRAY]")));
    }

    private static Throwable rootCause(Throwable e) {
        while (e.getCause() != null) {
            e = e.getCause();
        }
        return e;
    }

    public void testFromNestedXContent() throws IOException {
        SecureClusterStateSettings secrets = getSecureClusterStateSettings(NESTED_SECRETS);

        assertThat(secrets.getSettingNames(), containsInAnyOrder("foo.one.a", "goo.one.a", "goo.one.b", "goo.two"));
        assertThat(secrets.getString("foo.one.a").toString(), is("bar"));
        assertThat(secrets.getString("goo.one.a").toString(), is("oneA"));
        assertThat(secrets.getString("goo.one.b").toString(), is("oneB"));
        assertThat(secrets.getString("goo.two").toString(), is("two"));

        // throw on duplicate key
        var e = assertThrows(XContentParseException.class, () -> getSecureClusterStateSettings(NESTED_SECRETS.replace("goo", "foo")));
        assertThat(rootCause(e), hasToString(containsString("Some secrets were defined as both string and file secrets: [foo.one.a]")));

        // throw on structural duplicate
        e = assertThrows(XContentParseException.class, () -> getSecureClusterStateSettings(NESTED_SECRETS.replace("two", "one.a")));
        assertThat(rootCause(e), hasToString(containsString("Duplicate secret for key: goo.one.a")));
    }

    public void testCopyOf() {
        var otherSecureClusterStateSettings = SecureClusterStateSettings.copyOf(secureClusterStateSettings);
        assertThat(otherSecureClusterStateSettings, equalTo(secureClusterStateSettings));

        otherSecureClusterStateSettings.close();
        assertThat(otherSecureClusterStateSettings, not(equalTo(secureClusterStateSettings)));
        assertThat(otherSecureClusterStateSettings.getSettingNames(), is(secureClusterStateSettings.getSettingNames()));
    }

    public void testEquals() {
        var otherSecureClusterStateSettings = getSecureClusterStateSettings(FLAT_SECRETS);
        assertThat(otherSecureClusterStateSettings, equalTo(secureClusterStateSettings));

        assertThat(otherSecureClusterStateSettings, not(equalTo(SecureClusterStateSettings.EMPTY)));
        assertThat(getSecureClusterStateSettings(FLAT_SECRETS.replace("baz", "baz2")), not(equalTo(secureClusterStateSettings)));
        assertThat(getSecureClusterStateSettings(FLAT_SECRETS.replace("foo", "fuu")), not(equalTo(secureClusterStateSettings)));

        otherSecureClusterStateSettings.close();
        assertThat(otherSecureClusterStateSettings, not(equalTo(secureClusterStateSettings)));
        assertThat(otherSecureClusterStateSettings, not(equalTo(SecureClusterStateSettings.EMPTY)));

        secureClusterStateSettings.close();
        assertThat(otherSecureClusterStateSettings, equalTo(secureClusterStateSettings));
    }

    public void testConstructFromSecureSettings() {
        var mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setFile("foo", "bar".getBytes(StandardCharsets.UTF_8));
        mockSecureSettings.setFile("goo", "baz".getBytes(StandardCharsets.UTF_8));
        assertThat(new SecureClusterStateSettings(mockSecureSettings), equalTo(secureClusterStateSettings));
    }

    public void testClose() throws IOException {
        assertThat(secureClusterStateSettings.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(secureClusterStateSettings.getString("foo").toString(), is("bar"));
        secureClusterStateSettings.close();

        assertThat(secureClusterStateSettings.getSettingNames(), containsInAnyOrder("foo", "goo"));

        assertThrows(IllegalStateException.class, () -> secureClusterStateSettings.getString("foo"));
        assertThrows(IllegalStateException.class, () -> secureClusterStateSettings.getFile("goo"));
        assertThrows(IllegalStateException.class, () -> secureClusterStateSettings.getSHA256Digest("foo"));
    }

    public void testSerialize() throws Exception {
        final BytesStreamOutput out = new BytesStreamOutput();
        secureClusterStateSettings.writeTo(out);
        assertThat(new SecureClusterStateSettings(out.bytes().streamInput()), equalTo(secureClusterStateSettings));
    }

    private static String readString(InputStream is) {
        try {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    private static SecureClusterStateSettings getSecureClusterStateSettings(String json) {
        try {
            return SecureClusterStateSettings.fromXContent(JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
