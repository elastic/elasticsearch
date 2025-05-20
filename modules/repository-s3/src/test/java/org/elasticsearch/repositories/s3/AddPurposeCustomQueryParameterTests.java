/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import fixture.s3.S3HttpFixture;
import fixture.s3.S3HttpHandler;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.admin.cluster.repositories.put.PutRepositoryRequest;
import org.elasticsearch.action.admin.cluster.repositories.put.TransportPutRepositoryAction;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.create.TransportCreateSnapshotAction;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.hamcrest.Matcher;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

public class AddPurposeCustomQueryParameterTests extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.getPlugins(), S3RepositoryPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        final var secureSettings = new MockSecureSettings();
        for (final var clientName : List.of("default", "with_purpose", "without_purpose")) {
            secureSettings.setString(
                S3ClientSettings.ACCESS_KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                randomIdentifier()
            );
            secureSettings.setString(
                S3ClientSettings.SECRET_KEY_SETTING.getConcreteSettingForNamespace(clientName).getKey(),
                randomSecretKey()
            );
        }

        return Settings.builder()
            .put(super.nodeSettings())
            .put(S3ClientSettings.REGION.getConcreteSettingForNamespace("default").getKey(), randomIdentifier())
            .put(S3ClientSettings.ADD_PURPOSE_CUSTOM_QUERY_PARAMETER.getConcreteSettingForNamespace("with_purpose").getKey(), "true")
            .put(S3ClientSettings.ADD_PURPOSE_CUSTOM_QUERY_PARAMETER.getConcreteSettingForNamespace("without_purpose").getKey(), "false")
            .setSecureSettings(secureSettings)
            .build();
    }

    private static final Matcher<Iterable<? super String>> HAS_CUSTOM_QUERY_PARAMETER = hasItem(S3BlobStore.CUSTOM_QUERY_PARAMETER_PURPOSE);
    private static final Matcher<Iterable<? super String>> NO_CUSTOM_QUERY_PARAMETER = not(HAS_CUSTOM_QUERY_PARAMETER);

    public void testCustomQueryParameterConfiguration() throws Throwable {
        final var indexName = randomIdentifier();
        createIndex(indexName);
        prepareIndex(indexName).setSource("foo", "bar").get();

        final var bucket = randomIdentifier();
        final var basePath = randomIdentifier();

        runCustomQueryParameterTest(bucket, basePath, null, Settings.EMPTY, NO_CUSTOM_QUERY_PARAMETER);
        runCustomQueryParameterTest(bucket, basePath, "default", Settings.EMPTY, NO_CUSTOM_QUERY_PARAMETER);
        runCustomQueryParameterTest(bucket, basePath, "without_purpose", Settings.EMPTY, NO_CUSTOM_QUERY_PARAMETER);
        runCustomQueryParameterTest(bucket, basePath, "with_purpose", Settings.EMPTY, HAS_CUSTOM_QUERY_PARAMETER);

        final var falseRepositorySetting = Settings.builder().put("add_purpose_custom_query_parameter", false).build();
        final var trueRepositorySetting = Settings.builder().put("add_purpose_custom_query_parameter", true).build();
        for (final var clientName : new String[] { null, "default", "with_purpose", "without_purpose" }) {
            // client name doesn't matter if repository setting specified
            runCustomQueryParameterTest(bucket, basePath, clientName, falseRepositorySetting, NO_CUSTOM_QUERY_PARAMETER);
            runCustomQueryParameterTest(bucket, basePath, clientName, trueRepositorySetting, HAS_CUSTOM_QUERY_PARAMETER);
        }
    }

    private void runCustomQueryParameterTest(
        String bucket,
        String basePath,
        String clientName,
        Settings extraRepositorySettings,
        Matcher<Iterable<? super String>> queryParamMatcher
    ) throws Throwable {
        final var httpFixture = new S3HttpFixture(true, bucket, basePath, (key, token) -> true) {

            @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
            class AssertingHandler extends S3HttpHandler {
                AssertingHandler() {
                    super(bucket, basePath);
                }

                @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    try {
                        assertThat(parseRequest(exchange).queryParameters().keySet(), queryParamMatcher);
                        super.handle(exchange);
                    } catch (Error e) {
                        // HttpServer catches Throwable, so we must throw errors on another thread
                        ExceptionsHelper.maybeDieOnAnotherThread(e);
                        throw e;
                    }
                }
            }

            @SuppressForbidden(reason = "this test uses a HttpServer to emulate an S3 endpoint")
            @Override
            protected HttpHandler createHandler() {
                return new AssertingHandler();
            }
        };
        httpFixture.apply(new Statement() {
            @Override
            public void evaluate() {
                final var repoName = randomIdentifier();
                assertAcked(
                    client().execute(
                        TransportPutRepositoryAction.TYPE,
                        new PutRepositoryRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, repoName).type(S3Repository.TYPE)
                            .settings(
                                Settings.builder()
                                    .put("bucket", bucket)
                                    .put("base_path", basePath)
                                    .put("endpoint", httpFixture.getAddress())
                                    .put(clientName == null ? Settings.EMPTY : Settings.builder().put("client", clientName).build())
                                    .put(extraRepositorySettings)
                            )
                    )
                );

                assertEquals(
                    SnapshotState.SUCCESS,
                    client().execute(
                        TransportCreateSnapshotAction.TYPE,
                        new CreateSnapshotRequest(TEST_REQUEST_TIMEOUT, repoName, randomIdentifier()).waitForCompletion(true)
                    ).actionGet(SAFE_AWAIT_TIMEOUT).getSnapshotInfo().state()
                );
            }
        }, Description.EMPTY).evaluate();
    }

}
