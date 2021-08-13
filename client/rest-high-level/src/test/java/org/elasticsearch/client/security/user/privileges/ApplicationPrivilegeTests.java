/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security.user.privileges;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class ApplicationPrivilegeTests extends ESTestCase {

    public void testFromXContentAndToXContent() throws IOException {
        String json =
                "{\n"
                + "  \"application\" : \"myapp\",\n"
                + "  \"name\" : \"read\",\n"
                + "  \"actions\" : [\n"
                + "    \"data:read/*\",\n"
                + "    \"action:login\"\n"
                + "  ],\n"
                + "  \"metadata\" : {\n"
                + "    \"description\" : \"Read access to myapp\"\n"
                + "  }\n"
                + "}";
        final ApplicationPrivilege privilege = ApplicationPrivilege.fromXContent(XContentType.JSON.xContent().createParser(
            new NamedXContentRegistry(Collections.emptyList()), DeprecationHandler.IGNORE_DEPRECATIONS, json));
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("description", "Read access to myapp");
        final ApplicationPrivilege expectedPrivilege =
            new ApplicationPrivilege("myapp", "read", Arrays.asList("data:read/*", "action:login"), metadata);
        assertThat(privilege, equalTo(expectedPrivilege));

        XContentBuilder builder = privilege.toXContent(XContentFactory.jsonBuilder().prettyPrint(), ToXContent.EMPTY_PARAMS);
        String toJson = Strings.toString(builder);
        assertThat(toJson, equalTo(json));
    }

    public void testEmptyApplicationName() {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("description", "Read access to myapp");
        final String applicationName = randomBoolean() ? null : "";
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ApplicationPrivilege(applicationName, "read", Arrays.asList("data:read/*", "action:login"), metadata));
        assertThat(e.getMessage(), equalTo("application name must be provided"));
    }

    public void testEmptyPrivilegeName() {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("description", "Read access to myapp");
        final String privilegenName = randomBoolean() ? null : "";
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ApplicationPrivilege("myapp", privilegenName, Arrays.asList("data:read/*", "action:login"), metadata));
        assertThat(e.getMessage(), equalTo("privilege name must be provided"));
    }

    public void testEmptyActions() {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("description", "Read access to myapp");
        final List<String> actions = randomBoolean() ? null : Collections.emptyList();
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new ApplicationPrivilege("myapp", "read", actions, metadata));
        assertThat(e.getMessage(), equalTo("actions must be provided"));
    }

    public void testBuilder() {
        final Map<String, Object> metadata = new HashMap<>();
        metadata.put("description", "Read access to myapp");
        ApplicationPrivilege privilege = ApplicationPrivilege.builder()
            .application("myapp")
            .privilege("read")
            .actions("data:read/*", "action:login")
            .metadata(metadata)
            .build();
        assertThat(privilege.getApplication(), equalTo("myapp"));
        assertThat(privilege.getName(), equalTo("read"));
        assertThat(privilege.getActions(), containsInAnyOrder("data:read/*", "action:login"));
        assertThat(privilege.getMetadata(), equalTo(metadata));
    }
}
