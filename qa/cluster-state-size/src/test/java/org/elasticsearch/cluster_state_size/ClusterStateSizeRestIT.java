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

package org.elasticsearch.cluster_state_size;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.elasticsearch.Version;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ESClientYamlSuiteTestCase;

import java.util.Collections;

public class ClusterStateSizeRestIT extends ESClientYamlSuiteTestCase {

    public ClusterStateSizeRestIT(final ClientYamlTestCandidate candidate) {
        super(candidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return ESClientYamlSuiteTestCase.createParameters();
    }

    @Override
    protected RequestOptions getCatNodesVersionMasterRequestOptions() {
        final RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        final VersionSensitiveWarningsHandler handler = new VersionSensitiveWarningsHandler(Collections.singleton(Version.CURRENT));
        handler.current("es.cluster_state.size is deprecated and will be removed in 7.0.0");
        builder.setWarningsHandler(handler);
        return builder.build();
    }

    @Override
    protected boolean preserveClusterSettings() {
        // we did not add any cluster settings, we are the only test using this cluster, and trying to clean these will trigger a warning
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        // we did not add any templates, we are the only test using this cluster, and trying to clean these will trigger a warning
        return true;
    }

}
