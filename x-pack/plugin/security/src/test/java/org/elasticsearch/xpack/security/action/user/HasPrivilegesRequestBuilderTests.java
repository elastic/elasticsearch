/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.user;

import org.elasticsearch.action.admin.cluster.health.TransportClusterHealthAction;
import org.elasticsearch.action.admin.cluster.stats.TransportClusterStatsAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.Strings;
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequest;
import org.elasticsearch.xpack.core.security.action.user.HasPrivilegesRequestBuilder;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class HasPrivilegesRequestBuilderTests extends ESTestCase {

    public void testParseValidJsonWithClusterAndIndexPrivileges() throws Exception {
        String json = """
            {
              "cluster": [ "all" ],
              "index": [
                {
                  "names": [ ".kibana", ".reporting" ],
                  "privileges": [ "read", "write" ]
                },
                {
                  "names": [ ".security" ],
                  "privileges": [ "manage" ]
                }
              ]
            }""";

        final HasPrivilegesRequestBuilder builder = new HasPrivilegesRequestBuilder(mock(Client.class));
        builder.source("elastic", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON);

        final HasPrivilegesRequest request = builder.request();
        assertThat(request.clusterPrivileges().length, equalTo(1));
        assertThat(request.clusterPrivileges()[0], equalTo("all"));

        assertThat(request.indexPrivileges().length, equalTo(2));

        final RoleDescriptor.IndicesPrivileges privileges0 = request.indexPrivileges()[0];
        assertThat(privileges0.getIndices(), arrayContaining(".kibana", ".reporting"));
        assertThat(privileges0.getPrivileges(), arrayContaining("read", "write"));

        final RoleDescriptor.IndicesPrivileges privileges1 = request.indexPrivileges()[1];
        assertThat(privileges1.getIndices(), arrayContaining(".security"));
        assertThat(privileges1.getPrivileges(), arrayContaining("manage"));
    }

    public void testParseValidJsonWithJustIndexPrivileges() throws Exception {
        String json = """
            {
              "index": [
                {
                  "names": [ ".kibana", ".reporting" ],
                  "privileges": [ "read", "write" ]
                },
                {
                  "names": [ ".security" ],
                  "privileges": [ "manage" ]
                }
              ]
            }""";

        final HasPrivilegesRequestBuilder builder = new HasPrivilegesRequestBuilder(mock(Client.class));
        builder.source("elastic", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON);

        final HasPrivilegesRequest request = builder.request();
        assertThat(request.clusterPrivileges().length, equalTo(0));
        assertThat(request.indexPrivileges().length, equalTo(2));

        final RoleDescriptor.IndicesPrivileges privileges0 = request.indexPrivileges()[0];
        assertThat(privileges0.getIndices(), arrayContaining(".kibana", ".reporting"));
        assertThat(privileges0.getPrivileges(), arrayContaining("read", "write"));

        final RoleDescriptor.IndicesPrivileges privileges1 = request.indexPrivileges()[1];
        assertThat(privileges1.getIndices(), arrayContaining(".security"));
        assertThat(privileges1.getPrivileges(), arrayContaining("manage"));
    }

    public void testParseValidJsonWithJustClusterPrivileges() throws Exception {
        String json = Strings.format("""
            { "cluster": [ "manage","%s","%s"] }""", TransportClusterHealthAction.NAME, TransportClusterStatsAction.TYPE.name());

        final HasPrivilegesRequestBuilder builder = new HasPrivilegesRequestBuilder(mock(Client.class));
        builder.source("elastic", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON);

        final HasPrivilegesRequest request = builder.request();
        assertThat(request.indexPrivileges().length, equalTo(0));
        assertThat(
            request.clusterPrivileges(),
            arrayContaining("manage", TransportClusterHealthAction.NAME, TransportClusterStatsAction.TYPE.name())
        );
    }

    public void testUseOfFieldLevelSecurityThrowsException() throws Exception {
        String json = """
            {
              "index": [
                {
                  "names": [ "employees" ],
                  "privileges": [ "read", "write" ],
                  "field_security": {
                    "grant": [ "name", "department", "title" ]
                  }
                }
              ]
            }""";

        final HasPrivilegesRequestBuilder builder = new HasPrivilegesRequestBuilder(mock(Client.class));
        final ElasticsearchParseException parseException = expectThrows(
            ElasticsearchParseException.class,
            () -> builder.source("elastic", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON)
        );
        assertThat(parseException.getMessage(), containsString("[field_security]"));
    }

    public void testMissingPrivilegesThrowsException() throws Exception {
        String json = "{ }";
        final HasPrivilegesRequestBuilder builder = new HasPrivilegesRequestBuilder(mock(Client.class));
        final ElasticsearchParseException parseException = expectThrows(
            ElasticsearchParseException.class,
            () -> builder.source("elastic", new BytesArray(json.getBytes(StandardCharsets.UTF_8)), XContentType.JSON)
        );
        assertThat(parseException.getMessage(), containsString("[cluster,index,applications] are missing"));
    }
}
