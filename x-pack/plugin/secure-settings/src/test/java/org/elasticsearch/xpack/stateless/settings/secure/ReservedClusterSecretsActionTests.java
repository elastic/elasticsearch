/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.reservedstate.TransformState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;

import static org.elasticsearch.common.Strings.format;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class ReservedClusterSecretsActionTests extends ESTestCase {
    private TransformState processJSON(ReservedClusterSecretsAction action, TransformState prevState, String json) throws Exception {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            return action.transform(action.fromXContent(parser), prevState);
        }
    }

    public void testValidation() {
        TransformState prevState = new TransformState(ClusterState.builder(ClusterName.DEFAULT).build(), Collections.emptySet());
        ReservedClusterSecretsAction action = new ReservedClusterSecretsAction();

        String json = """
            {
                "not_a_field": {"secure.test": "test"}
            }""";

        assertThat(
            expectThrows(IllegalArgumentException.class, () -> processJSON(action, prevState, json)).getMessage(),
            is("[2:5] [secrets_parser] unknown field [not_a_field]")
        );
    }

    public void testSetAndOverwriteSettings() throws Exception {
        TransformState prevState = new TransformState(ClusterState.builder(ClusterName.DEFAULT).build(), Collections.emptySet());
        ReservedClusterSecretsAction action = new ReservedClusterSecretsAction();

        {
            String json = format("""
                {
                    "string_secrets": {"secure.test1": "test1"},
                    "file_secrets": {"secure.test2": "%s"}
                }""", new String(Base64.getEncoder().encode("test2".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));

            prevState = processJSON(action, prevState, json);
            ClusterSecrets clusterSecrets = prevState.state().custom(ClusterSecrets.TYPE);
            assertNotNull(clusterSecrets);
            assertEquals(1L, clusterSecrets.getVersion());
            assertEquals("test1", clusterSecrets.getSettings().getString("secure.test1").toString());
            assertEquals("test2", new String(clusterSecrets.getSettings().getFile("secure.test2").readAllBytes(), StandardCharsets.UTF_8));
        }
        {
            String json = format("""
                {
                    "string_secrets": {"secure.test1": "test3"},
                    "file_secrets": {"secure.test3": "%s"}
                }""", new String(Base64.getEncoder().encode("test4".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));

            TransformState transformState = processJSON(action, prevState, json);
            ClusterSecrets clusterSecrets = transformState.state().custom(ClusterSecrets.TYPE);
            assertNotNull(clusterSecrets);
            assertEquals(2L, clusterSecrets.getVersion());
            assertEquals("test3", clusterSecrets.getSettings().getString("secure.test1").toString());
            assertEquals("test4", new String(clusterSecrets.getSettings().getFile("secure.test3").readAllBytes(), StandardCharsets.UTF_8));
            assertNull(clusterSecrets.getSettings().getString("secure.test2"));
        }
    }

    public void testDuplicateSecretFails() {
        TransformState prevState = new TransformState(ClusterState.builder(ClusterName.DEFAULT).build(), Collections.emptySet());
        ReservedClusterSecretsAction action = new ReservedClusterSecretsAction();

        String json = format("""
            {
                "string_secrets": {"secure.test1": "test1"},
                "file_secrets": {"secure.test1": "%s"}
            }""", new String(Base64.getEncoder().encode("test2".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));

        assertThat(
            expectThrows(XContentParseException.class, () -> processJSON(action, prevState, json)).getCause().getCause().getMessage(),
            both(containsString("both string and file secret")).and(containsString("[secure.test1]"))
        );
    }

    public void testVersionIncrements() throws Exception {
        TransformState prevState = new TransformState(ClusterState.builder(ClusterName.DEFAULT).build(), Collections.emptySet());
        ReservedClusterSecretsAction action = new ReservedClusterSecretsAction();

        String json1 = format("""
            {
                "string_secrets": {"secure.test1": "test1"},
                "file_secrets": {"secure.test2": "%s"}
            }""", new String(Base64.getEncoder().encode("test2".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));

        for (int i = 1; i <= 3; i++) {
            prevState = processJSON(action, prevState, json1);
            ClusterSecrets secrets1 = prevState.state().custom(ClusterSecrets.TYPE);
            assertNotNull(secrets1);
            assertEquals(1L, secrets1.getVersion()); // idempotent updates, version only increments on changes
        }

        String json2 = format("""
            {
                "string_secrets": {"secure.test1": "test3"},
                "file_secrets": {"secure.test2": "%s"}
            }""", new String(Base64.getEncoder().encode("test2".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));

        TransformState state2 = processJSON(action, prevState, json2);
        ClusterSecrets secrets2 = state2.state().custom(ClusterSecrets.TYPE);
        assertNotNull(secrets2);
        assertEquals(2L, secrets2.getVersion());

        String json3 = """
            {
                "string_secrets": {"secure.test4": "test4"},
                "file_secrets": {}
            }""";

        TransformState state3 = processJSON(action, state2, json3);
        ClusterSecrets secrets3 = state3.state().custom(ClusterSecrets.TYPE);
        assertNotNull(secrets3);
        assertEquals(3L, secrets3.getVersion());
    }

    public void testRemove() throws Exception {
        TransformState prevState = new TransformState(ClusterState.builder(ClusterName.DEFAULT).build(), Collections.emptySet());
        ReservedClusterSecretsAction action = new ReservedClusterSecretsAction();

        String json = format("""
            {
                "string_secrets": {"secure.test1": "test1"},
                "file_secrets": {"secure.test2": "%s"}
            }""", new String(Base64.getEncoder().encode("test2".getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8));

        prevState = processJSON(action, prevState, json);
        ClusterSecrets clusterSecrets = prevState.state().custom(ClusterSecrets.TYPE);
        assertNotNull(clusterSecrets);

        ClusterState removedState = action.remove(prevState);
        assertNull(removedState.custom(ClusterSecrets.TYPE));
    }
}
