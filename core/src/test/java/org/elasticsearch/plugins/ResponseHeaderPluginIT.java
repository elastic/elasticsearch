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
package org.elasticsearch.plugins;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.responseheader.TestResponseHeaderPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.Test;

import java.util.Collection;

import static org.elasticsearch.rest.RestStatus.OK;
import static org.elasticsearch.rest.RestStatus.UNAUTHORIZED;
import static org.elasticsearch.test.ESIntegTestCase.Scope;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasStatus;
import static org.hamcrest.Matchers.equalTo;

/**
 * Test a rest action that sets special response headers
 */
@ClusterScope(scope = Scope.SUITE, numDataNodes = 1)
public class ResponseHeaderPluginIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.settingsBuilder()
                .put(super.nodeSettings(nodeOrdinal))
                .put("force.http.enabled", true)
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(TestResponseHeaderPlugin.class);
    }

    @Test
    public void testThatSettingHeadersWorks() throws Exception {
        ensureGreen();
        HttpResponse response = httpClient().method("GET").path("/_protected").execute();
        assertThat(response, hasStatus(UNAUTHORIZED));
        assertThat(response.getHeaders().get("Secret"), equalTo("required"));

        HttpResponse authResponse = httpClient().method("GET").path("/_protected").addHeader("Secret", "password").execute();
        assertThat(authResponse, hasStatus(OK));
        assertThat(authResponse.getHeaders().get("Secret"), equalTo("granted"));
    }
    
}