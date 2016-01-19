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

package org.elasticsearch.test;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.Collections.unmodifiableList;

/**
 * Plugin that registers a filter that records actions.
 */
public class ActionRecordingPlugin extends Plugin {
    /**
     * Fetch all the requests recorded by the test plugin. The list is an
     * immutable, moment in time snapshot.
     */
    public static List<ActionRequest<?>> allRequests() {
        List<ActionRequest<?>> requests = new ArrayList<>();
        for (RecordingFilter filter : ESIntegTestCase.internalCluster().getInstances(RecordingFilter.class)) {
            requests.addAll(filter.requests);
        }
        return unmodifiableList(requests);
    }

    /**
     * Fetch all requests recorded by the test plugin of a certain type. The
     * list is an immutable, moment in time snapshot.
     */
    public static <T> List<T> requestsOfType(Class<T> type) {
        List<T> requests = new ArrayList<>();
        for (RecordingFilter filter : ESIntegTestCase.internalCluster().getInstances(RecordingFilter.class)) {
            for (ActionRequest<?> request : filter.requests) {
                if (type.isInstance(request)) {
                    requests.add(type.cast(request));
                }
            }
        }
        return unmodifiableList(requests);
    }

    /**
     * Clear all the recorded requests. Use between test methods that shared a
     * suite scoped cluster.
     */
    public static void clear() {
        for (RecordingFilter filter : ESIntegTestCase.internalCluster().getInstances(RecordingFilter.class)) {
            filter.requests.clear();
        }
    }

    @Override
    public String name() {
        return "test-action-logging";
    }

    @Override
    public String description() {
        return "Test action logging";
    }

    @Override
    public Collection<Module> nodeModules() {
        return Collections.<Module>singletonList(new ActionRecordingModule());
    }

    public void onModule(ActionModule module) {
        module.registerFilter(RecordingFilter.class);
    }

    public static class ActionRecordingModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(RecordingFilter.class).asEagerSingleton();
        }

    }

    public static class RecordingFilter extends ActionFilter.Simple {
        private final List<ActionRequest<?>> requests = new CopyOnWriteArrayList<>();

        @Inject
        public RecordingFilter(Settings settings) {
            super(settings);
        }

        public List<ActionRequest<?>> getRequests() {
            return new ArrayList<>(requests);
        }

        @Override
        public int order() {
            return 999;
        }

        @Override
        protected boolean apply(String action, ActionRequest<?> request, ActionListener<?> listener) {
            requests.add(request);
            return true;
        }

        @Override
        protected boolean apply(String action, ActionResponse response, ActionListener<?> listener) {
            return true;
        }
    }
}
