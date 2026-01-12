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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class SecureClusterStateSettingsTests extends ESTestCase {

    private static final String JSON_SECRETS = Strings.format("""
        {
          "file_secrets": {
            "foo": "%s"
          },
          "string_secrets": {
            "goo": "baz"
          }
        }
        """, Base64.getEncoder().encodeToString("bar".getBytes(StandardCharsets.UTF_8)));

    private static final String JSON_DUPLICATE_KEYS = Strings.format("""
        {
          "file_secrets": {
            "foo": "%s"
          },
          "string_secrets": {
            "foo": "bar"
          }
        }
        """, Base64.getEncoder().encodeToString("bar".getBytes(StandardCharsets.UTF_8)));

    private final SecureClusterStateSettings secureClusterStateSettings = getSecureClusterStateSettings(JSON_SECRETS);

    public void testFromXContent() throws IOException {
        assertThat(secureClusterStateSettings.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(secureClusterStateSettings.getString("foo").toString(), is("bar"));
        assertThat(secureClusterStateSettings.getString("goo").toString(), is("baz"));
        assertThat(readString(secureClusterStateSettings.getFile("foo")), is("bar"));
        assertThat(readString(secureClusterStateSettings.getFile("goo")), is("baz"));

        assertThrows(XContentParseException.class, () -> getSecureClusterStateSettings(JSON_DUPLICATE_KEYS));
    }

    public void testCopyOf() {
        var otherSecureClusterStateSettings = SecureClusterStateSettings.copyOf(secureClusterStateSettings);
        assertThat(otherSecureClusterStateSettings, equalTo(secureClusterStateSettings));

        otherSecureClusterStateSettings.close();
        assertThat(otherSecureClusterStateSettings, not(equalTo(secureClusterStateSettings)));
        assertThat(otherSecureClusterStateSettings.getSettingNames(), is(secureClusterStateSettings.getSettingNames()));
    }

    public void testEquals() {
        var otherSecureClusterStateSettings = getSecureClusterStateSettings(JSON_SECRETS);
        assertThat(otherSecureClusterStateSettings, equalTo(secureClusterStateSettings));

        assertThat(otherSecureClusterStateSettings, not(equalTo(SecureClusterStateSettings.EMPTY)));
        assertThat(getSecureClusterStateSettings(JSON_SECRETS.replace("baz", "baz2")), not(equalTo(secureClusterStateSettings)));
        assertThat(getSecureClusterStateSettings(JSON_SECRETS.replace("foo", "fuu")), not(equalTo(secureClusterStateSettings)));

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
