/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.security;

import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class PutUserRequestTests extends ESTestCase {

    public void testBuildRequestWithPassword() throws Exception {
        final User user = new User("hawkeye", Arrays.asList("kibana_user", "avengers"),
            Collections.singletonMap("status", "active"), "Clinton Barton", null);
        final char[] password = "f@rmb0y".toCharArray();
        final PutUserRequest request = PutUserRequest.withPassword(user, password, true, RefreshPolicy.IMMEDIATE);
        String json = Strings.toString(request);
        final Map<String, Object> requestAsMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), json, false);
        assertThat(requestAsMap.get("username"), is("hawkeye"));
        assertThat(requestAsMap.get("roles"), instanceOf(List.class));
        assertThat((List<?>) requestAsMap.get("roles"), containsInAnyOrder("kibana_user", "avengers"));
        assertThat(requestAsMap.get("password"), is("f@rmb0y"));
        assertThat(requestAsMap.containsKey("password_hash"), is(false));
        assertThat(requestAsMap.get("full_name"), is("Clinton Barton"));
        assertThat(requestAsMap.containsKey("email"), is(false));
        assertThat(requestAsMap.get("enabled"), is(true));
        assertThat(requestAsMap.get("metadata"), instanceOf(Map.class));
        final Map<?, ?> metadata = (Map<?, ?>) requestAsMap.get("metadata");
        assertThat(metadata.size(), is(1));
        assertThat(metadata.get("status"), is("active"));
    }

    public void testBuildRequestWithPasswordHash() throws Exception {
        final User user = new User("hawkeye", Arrays.asList("kibana_user", "avengers"),
            Collections.singletonMap("status", "active"), "Clinton Barton", null);
        final char[] passwordHash = "$2a$04$iu1G4x3ZKVDNi6egZIjkFuIPja6elQXiBF1LdRVauV4TGog6FYOpi".toCharArray();
        final PutUserRequest request = PutUserRequest.withPasswordHash(user, passwordHash, true, RefreshPolicy.IMMEDIATE);
        String json = Strings.toString(request);
        final Map<String, Object> requestAsMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), json, false);
        assertThat(requestAsMap.get("username"), is("hawkeye"));
        assertThat(requestAsMap.get("roles"), instanceOf(List.class));
        assertThat((List<?>) requestAsMap.get("roles"), containsInAnyOrder("kibana_user", "avengers"));
        assertThat(requestAsMap.get("password_hash"), is("$2a$04$iu1G4x3ZKVDNi6egZIjkFuIPja6elQXiBF1LdRVauV4TGog6FYOpi"));
        assertThat(requestAsMap.containsKey("password"), is(false));
        assertThat(requestAsMap.get("full_name"), is("Clinton Barton"));
        assertThat(requestAsMap.containsKey("email"), is(false));
        assertThat(requestAsMap.get("enabled"), is(true));
        assertThat(requestAsMap.get("metadata"), instanceOf(Map.class));
        final Map<?, ?> metadata = (Map<?, ?>) requestAsMap.get("metadata");
        assertThat(metadata.size(), is(1));
        assertThat(metadata.get("status"), is("active"));
    }

    public void testBuildRequestForUpdateOnly() throws Exception {
        final User user = new User("hawkeye", Arrays.asList("kibana_user", "avengers"),
            Collections.singletonMap("status", "active"), "Clinton Barton", null);
        final char[] passwordHash = "$2a$04$iu1G4x3ZKVDNi6egZIjkFuIPja6elQXiBF1LdRVauV4TGog6FYOpi".toCharArray();
        final PutUserRequest request = PutUserRequest.updateUser(user, true, RefreshPolicy.IMMEDIATE);
        String json = Strings.toString(request);
        final Map<String, Object> requestAsMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), json, false);
        assertThat(requestAsMap.get("username"), is("hawkeye"));
        assertThat(requestAsMap.get("roles"), instanceOf(List.class));
        assertThat((List<?>) requestAsMap.get("roles"), containsInAnyOrder("kibana_user", "avengers"));
        assertThat(requestAsMap.containsKey("password"), is(false));
        assertThat(requestAsMap.containsKey("password_hash"), is(false));
        assertThat(requestAsMap.get("full_name"), is("Clinton Barton"));
        assertThat(requestAsMap.containsKey("email"), is(false));
        assertThat(requestAsMap.get("enabled"), is(true));
        assertThat(requestAsMap.get("metadata"), instanceOf(Map.class));
        final Map<?, ?> metadata = (Map<?, ?>) requestAsMap.get("metadata");
        assertThat(metadata.size(), is(1));
        assertThat(metadata.get("status"), is("active"));
    }

}
