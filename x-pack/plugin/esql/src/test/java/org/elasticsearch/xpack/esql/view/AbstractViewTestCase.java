/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.view;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.esql.plugin.EsqlFeatures.ESQL_VIEWS_FEATURE_FLAG;
import static org.elasticsearch.xpack.esql.view.ViewService.ViewServiceConfig.DEFAULT;

public abstract class AbstractViewTestCase extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateView.class);
    }

    protected ViewService viewService(ProjectResolver projectResolver) {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        FeatureService featureService = getInstanceFromNode(FeatureService.class);
        return new ClusterViewService(new EsqlFunctionRegistry(), clusterService, featureService, projectResolver, DEFAULT);
    }

    protected class TestViewsApi {
        protected final ViewService viewService;
        protected final ProjectId projectId;

        public TestViewsApi() {
            if (ESQL_VIEWS_FEATURE_FLAG.isEnabled() == false) {
                // The TestResponseCapture implementation waits forever if views are not enabled, so lets rather fail early
                throw new IllegalStateException("Views tests cannot run in release mode yet");
            }
            ProjectResolver projectResolver = getInstanceFromNode(ProjectResolver.class);
            this.viewService = viewService(projectResolver);
            this.projectId = projectResolver.getProjectId();
        }

        protected AtomicReference<Exception> save(String name, View policy) throws InterruptedException {
            TestResponseCapture<Void> responseCapture = new TestResponseCapture<>();
            viewService.put(projectId, name, policy, responseCapture);
            responseCapture.latch.await();
            return responseCapture.error;
        }

        protected void delete(String name) throws Exception {
            TestResponseCapture<Void> responseCapture = new TestResponseCapture<>();
            viewService.delete(projectId, name, responseCapture);
            responseCapture.latch.await();
            if (responseCapture.error.get() != null) {
                throw responseCapture.error.get();
            }
        }

        public Map<String, View> get(String... names) throws Exception {
            if (names == null || names.length == 1 && names[0] == null) {
                // This is only for consistent testing, in production this is already checked in the REST API
                throw new IllegalArgumentException("name is missing or empty");
            }
            TestResponseCapture<GetViewAction.Response> responseCapture = new TestResponseCapture<>();
            TransportGetViewAction getViewAction = getInstanceFromNode(TransportGetViewAction.class);
            GetViewAction.Request request = new GetViewAction.Request(TimeValue.THIRTY_SECONDS, names);
            getViewAction.doExecute(null, request, responseCapture);
            if (responseCapture.error.get() != null) {
                throw responseCapture.error.get();
            }
            return responseCapture.response.getViews();
        }
    }

    protected static class TestResponseCapture<T> implements ActionListener<T> {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        T response;

        @Override
        public void onResponse(T response) {
            latch.countDown();
            this.response = response;
        }

        @Override
        public void onFailure(Exception e) {
            error.set(e);
            latch.countDown();
        }
    }
}
