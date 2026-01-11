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
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

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

    private static final MockSecureSettings MOCK_SECURE_SETTINGS = new MockSecureSettings();
    static {
        MOCK_SECURE_SETTINGS.setFile("foo", "bar".getBytes(StandardCharsets.UTF_8));
        MOCK_SECURE_SETTINGS.setFile("goo", "baz".getBytes(StandardCharsets.UTF_8));
    }

    public void testFromXContent() throws IOException {
        assertThat(
            SecureClusterStateSettings.fromXContent(createParser(JsonXContent.jsonXContent, JSON_SECRETS)),
            containsSecrets(Map.of("foo", "bar", "goo", "baz"))
        );

        assertThrows(
            XContentParseException.class,
            () -> SecureClusterStateSettings.fromXContent(createParser(JsonXContent.jsonXContent, JSON_DUPLICATE_KEYS))
        );
    }

    public void testConstructFromSecureSettings() {
        assertThat(new SecureClusterStateSettings(MOCK_SECURE_SETTINGS), containsSecrets(Map.of("foo", "bar", "goo", "baz")));
    }

    public void testClose() {
        var secrets = new SecureClusterStateSettings(MOCK_SECURE_SETTINGS);
        assertThat(secrets.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThat(secrets.getString("foo").toString(), is("bar"));
        secrets.close();

        assertThat(secrets.getSettingNames(), containsInAnyOrder("foo", "goo"));
        assertThrows(IllegalStateException.class, () -> secrets.getString("foo"));
        assertThrows(IllegalStateException.class, () -> secrets.getFile("goo"));
        assertThrows(IllegalStateException.class, () -> secrets.getSHA256Digest("foo"));
    }

    public void testSerialize() throws Exception {
        final BytesStreamOutput out = new BytesStreamOutput();
        new SecureClusterStateSettings(MOCK_SECURE_SETTINGS).writeTo(out);
        assertThat(new SecureClusterStateSettings(out.bytes().streamInput()), containsSecrets(Map.of("foo", "bar", "goo", "baz")));
    }

    private static Matcher<SecureClusterStateSettings> containsSecrets(Map<String, String> secrets) {
        List<Matcher<? super SecureClusterStateSettings>> matchers = new ArrayList<>();
        matchers.add(
            transformedMatch("setting names", SecureClusterStateSettings::getSettingNames, containsInAnyOrder(secrets.keySet().toArray()))
        );
        matchers.add(transformedMatch("loaded", SecureClusterStateSettings::isLoaded, is(true)));
        for (Map.Entry<String, String> e : secrets.entrySet()) {
            matchers.add(transformedMatch("string secret " + e.getKey(), s -> s.getString(e.getKey()).toString(), is(e.getValue())));
            matchers.add(transformedMatch("file secret " + e.getKey(), s -> readString(s.getFile(e.getKey())), is(e.getValue())));
        }
        return allOf(matchers);
    }

    private static String readString(InputStream is) {
        try {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }
}
