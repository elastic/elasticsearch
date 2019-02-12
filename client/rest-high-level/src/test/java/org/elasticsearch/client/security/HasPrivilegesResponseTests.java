/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.client.security;

import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class HasPrivilegesResponseTests extends ESTestCase {

    public void testParseValidResponse() throws IOException {
        String json = "{" +
            " \"username\": \"namor\"," +
            " \"has_all_requested\": false," +
            " \"cluster\" : {" +
            "   \"manage\" : false," +
            "   \"monitor\" : true" +
            " }," +
            " \"index\" : {" +
            "   \"index-01\": {" +
            "     \"read\" : true," +
            "     \"write\" : false" +
            "   }," +
            "   \"index-02\": {" +
            "     \"read\" : true," +
            "     \"write\" : true" +
            "   }," +
            "   \"index-03\": {" +
            "     \"read\" : false," +
            "     \"write\" : false" +
            "   }" +
            " }," +
            " \"application\" : {" +
            "   \"app01\" : {" +
            "     \"/object/1\" : {" +
            "       \"read\" : true," +
            "       \"write\" : false" +
            "     }," +
            "     \"/object/2\" : {" +
            "       \"read\" : true," +
            "       \"write\" : true" +
            "     }" +
            "   }," +
            "   \"app02\" : {" +
            "     \"/object/1\" : {" +
            "       \"read\" : false," +
            "       \"write\" : false" +
            "     }," +
            "     \"/object/3\" : {" +
            "       \"read\" : false," +
            "       \"write\" : true" +
            "     }" +
            "   }" +
            " }" +
            "}";
        final XContentParser parser = createParser(XContentType.JSON.xContent(), json);
        HasPrivilegesResponse response = HasPrivilegesResponse.fromXContent(parser);

        assertThat(response.getUsername(), Matchers.equalTo("namor"));
        assertThat(response.hasAllRequested(), Matchers.equalTo(false));

        assertThat(response.getClusterPrivileges().keySet(), Matchers.containsInAnyOrder("monitor", "manage"));
        assertThat(response.hasClusterPrivilege("monitor"), Matchers.equalTo(true));
        assertThat(response.hasClusterPrivilege("manage"), Matchers.equalTo(false));

        assertThat(response.getIndexPrivileges().keySet(), Matchers.containsInAnyOrder("index-01", "index-02", "index-03"));
        assertThat(response.hasIndexPrivilege("index-01", "read"), Matchers.equalTo(true));
        assertThat(response.hasIndexPrivilege("index-01", "write"), Matchers.equalTo(false));
        assertThat(response.hasIndexPrivilege("index-02", "read"), Matchers.equalTo(true));
        assertThat(response.hasIndexPrivilege("index-02", "write"), Matchers.equalTo(true));
        assertThat(response.hasIndexPrivilege("index-03", "read"), Matchers.equalTo(false));
        assertThat(response.hasIndexPrivilege("index-03", "write"), Matchers.equalTo(false));

        assertThat(response.getApplicationPrivileges().keySet(), Matchers.containsInAnyOrder("app01", "app02"));
        assertThat(response.hasApplicationPrivilege("app01", "/object/1", "read"), Matchers.equalTo(true));
        assertThat(response.hasApplicationPrivilege("app01", "/object/1", "write"), Matchers.equalTo(false));
        assertThat(response.hasApplicationPrivilege("app01", "/object/2", "read"), Matchers.equalTo(true));
        assertThat(response.hasApplicationPrivilege("app01", "/object/2", "write"), Matchers.equalTo(true));
        assertThat(response.hasApplicationPrivilege("app02", "/object/1", "read"), Matchers.equalTo(false));
        assertThat(response.hasApplicationPrivilege("app02", "/object/1", "write"), Matchers.equalTo(false));
        assertThat(response.hasApplicationPrivilege("app02", "/object/3", "read"), Matchers.equalTo(false));
        assertThat(response.hasApplicationPrivilege("app02", "/object/3", "write"), Matchers.equalTo(true));
    }

    public void testHasClusterPrivilege() {
        final Map<String, Boolean> cluster = MapBuilder.<String, Boolean>newMapBuilder()
            .put("a", true)
            .put("b", false)
            .put("c", false)
            .put("d", true)
            .map();
        final HasPrivilegesResponse response = new HasPrivilegesResponse("x", false, cluster, emptyMap(), emptyMap());
        assertThat(response.hasClusterPrivilege("a"), Matchers.is(true));
        assertThat(response.hasClusterPrivilege("b"), Matchers.is(false));
        assertThat(response.hasClusterPrivilege("c"), Matchers.is(false));
        assertThat(response.hasClusterPrivilege("d"), Matchers.is(true));

        final IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> response.hasClusterPrivilege("e"));
        assertThat(iae.getMessage(), Matchers.containsString("[e]"));
        assertThat(iae.getMessage().toLowerCase(Locale.ROOT), Matchers.containsString("cluster privilege"));
    }

    public void testHasIndexPrivilege() {
        final Map<String, Map<String, Boolean>> index = MapBuilder.<String, Map<String, Boolean>>newMapBuilder()
            .put("i1", Collections.singletonMap("read", true))
            .put("i2", Collections.singletonMap("read", false))
            .put("i3", MapBuilder.<String, Boolean>newMapBuilder().put("read", true).put("write", true).map())
            .put("i4", MapBuilder.<String, Boolean>newMapBuilder().put("read", true).put("write", false).map())
            .put("i*", MapBuilder.<String, Boolean>newMapBuilder().put("read", false).put("write", false).map())
            .map();
        final HasPrivilegesResponse response = new HasPrivilegesResponse("x", false, emptyMap(), index, emptyMap());
        assertThat(response.hasIndexPrivilege("i1", "read"), Matchers.is(true));
        assertThat(response.hasIndexPrivilege("i2", "read"), Matchers.is(false));
        assertThat(response.hasIndexPrivilege("i3", "read"), Matchers.is(true));
        assertThat(response.hasIndexPrivilege("i3", "write"), Matchers.is(true));
        assertThat(response.hasIndexPrivilege("i4", "read"), Matchers.is(true));
        assertThat(response.hasIndexPrivilege("i4", "write"), Matchers.is(false));
        assertThat(response.hasIndexPrivilege("i*", "read"), Matchers.is(false));
        assertThat(response.hasIndexPrivilege("i*", "write"), Matchers.is(false));

        final IllegalArgumentException iae1 = expectThrows(IllegalArgumentException.class, () -> response.hasIndexPrivilege("i0", "read"));
        assertThat(iae1.getMessage(), Matchers.containsString("index [i0]"));

        final IllegalArgumentException iae2 = expectThrows(IllegalArgumentException.class, () -> response.hasIndexPrivilege("i1", "write"));
        assertThat(iae2.getMessage().toLowerCase(Locale.ROOT), Matchers.containsString("privilege [write]"));
        assertThat(iae2.getMessage(), Matchers.containsString("index [i1]"));
    }

    public void testHasApplicationPrivilege() {
        final Map<String, Map<String, Boolean>> app1 = MapBuilder.<String, Map<String, Boolean>>newMapBuilder()
            .put("/data/1", Collections.singletonMap("read", true))
            .put("/data/2", Collections.singletonMap("read", false))
            .put("/data/3", MapBuilder.<String, Boolean>newMapBuilder().put("read", true).put("write", true).map())
            .put("/data/4", MapBuilder.<String, Boolean>newMapBuilder().put("read", true).put("write", false).map())
            .map();
        final Map<String, Map<String, Boolean>> app2 = MapBuilder.<String, Map<String, Boolean>>newMapBuilder()
            .put("/action/1", Collections.singletonMap("execute", true))
            .put("/action/*", Collections.singletonMap("execute", false))
            .map();
        Map<String, Map<String, Map<String, Boolean>>> appPrivileges = new HashMap<>();
        appPrivileges.put("a1", app1);
        appPrivileges.put("a2", app2);
        final HasPrivilegesResponse response = new HasPrivilegesResponse("x", false, emptyMap(), emptyMap(), appPrivileges);
        assertThat(response.hasApplicationPrivilege("a1", "/data/1", "read"), Matchers.is(true));
        assertThat(response.hasApplicationPrivilege("a1", "/data/2", "read"), Matchers.is(false));
        assertThat(response.hasApplicationPrivilege("a1", "/data/3", "read"), Matchers.is(true));
        assertThat(response.hasApplicationPrivilege("a1", "/data/3", "write"), Matchers.is(true));
        assertThat(response.hasApplicationPrivilege("a1", "/data/4", "read"), Matchers.is(true));
        assertThat(response.hasApplicationPrivilege("a1", "/data/4", "write"), Matchers.is(false));
        assertThat(response.hasApplicationPrivilege("a2", "/action/1", "execute"), Matchers.is(true));
        assertThat(response.hasApplicationPrivilege("a2", "/action/*", "execute"), Matchers.is(false));

        final IllegalArgumentException iae1 = expectThrows(IllegalArgumentException.class,
            () -> response.hasApplicationPrivilege("a0", "/data/1", "read"));
        assertThat(iae1.getMessage().toLowerCase(Locale.ROOT), Matchers.containsString("application [a0]"));

        final IllegalArgumentException iae2 = expectThrows(IllegalArgumentException.class,
            () -> response.hasApplicationPrivilege("a1", "/data/0", "read"));
        assertThat(iae2.getMessage().toLowerCase(Locale.ROOT), Matchers.containsString("application [a1]"));
        assertThat(iae2.getMessage().toLowerCase(Locale.ROOT), Matchers.containsString("resource [/data/0]"));

        final IllegalArgumentException iae3 = expectThrows(IllegalArgumentException.class,
            () -> response.hasApplicationPrivilege("a1", "/action/1", "execute"));
        assertThat(iae3.getMessage().toLowerCase(Locale.ROOT), Matchers.containsString("application [a1]"));
        assertThat(iae3.getMessage().toLowerCase(Locale.ROOT), Matchers.containsString("resource [/action/1]"));

        final IllegalArgumentException iae4 = expectThrows(IllegalArgumentException.class,
            () -> response.hasApplicationPrivilege("a1", "/data/1", "write"));
        assertThat(iae4.getMessage().toLowerCase(Locale.ROOT), Matchers.containsString("application [a1]"));
        assertThat(iae4.getMessage().toLowerCase(Locale.ROOT), Matchers.containsString("resource [/data/1]"));
        assertThat(iae4.getMessage().toLowerCase(Locale.ROOT), Matchers.containsString("privilege [write]"));
    }

    public void testEqualsAndHashCode() {
        final HasPrivilegesResponse response = randomResponse();
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(response, this::copy, this::mutate);
    }

    private HasPrivilegesResponse copy(HasPrivilegesResponse response) {
        return new HasPrivilegesResponse(response.getUsername(),
            response.hasAllRequested(),
            response.getClusterPrivileges(),
            response.getIndexPrivileges(),
            response.getApplicationPrivileges());
    }

    private HasPrivilegesResponse mutate(HasPrivilegesResponse request) {
        switch (randomIntBetween(1, 5)) {
            case 1:
                return new HasPrivilegesResponse("_" + request.getUsername(), request.hasAllRequested(),
                    request.getClusterPrivileges(), request.getIndexPrivileges(), request.getApplicationPrivileges());
            case 2:
                return new HasPrivilegesResponse(request.getUsername(), request.hasAllRequested() == false,
                    request.getClusterPrivileges(), request.getIndexPrivileges(), request.getApplicationPrivileges());
            case 3:
                return new HasPrivilegesResponse(request.getUsername(), request.hasAllRequested(),
                    emptyMap(), request.getIndexPrivileges(), request.getApplicationPrivileges());
            case 4:
                return new HasPrivilegesResponse(request.getUsername(), request.hasAllRequested(),
                    request.getClusterPrivileges(), emptyMap(), request.getApplicationPrivileges());
            case 5:
                return new HasPrivilegesResponse(request.getUsername(), request.hasAllRequested(),
                    request.getClusterPrivileges(), request.getIndexPrivileges(), emptyMap());
        }
        throw new IllegalStateException("The universe is broken (or the RNG is)");
    }

    private HasPrivilegesResponse randomResponse() {
        final Map<String, Boolean> cluster = randomPrivilegeMap();
        final Map<String, Map<String, Boolean>> index = randomResourceMap();

        final Map<String, Map<String, Map<String, Boolean>>> application = new HashMap<>();
        for (String app : randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(3, 6).toLowerCase(Locale.ROOT))) {
            application.put(app, randomResourceMap());
        }
        return new HasPrivilegesResponse(randomAlphaOfLengthBetween(3, 8), randomBoolean(), cluster, index, application);
    }

    private Map<String, Map<String, Boolean>> randomResourceMap() {
        final Map<String, Map<String, Boolean>> resource = new HashMap<>();
        for (String res : randomArray(1, 3, String[]::new, () -> randomAlphaOfLengthBetween(5, 8))) {
            resource.put(res, randomPrivilegeMap());
        }
        return resource;
    }

    private Map<String, Boolean> randomPrivilegeMap() {
        final Map<String, Boolean> map = new HashMap<>();
        for (String privilege : randomArray(1, 6, String[]::new, () -> randomAlphaOfLengthBetween(3, 12))) {
            map.put(privilege, randomBoolean());
        }
        return map;
    }

}
