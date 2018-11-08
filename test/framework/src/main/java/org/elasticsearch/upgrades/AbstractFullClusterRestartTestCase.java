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

package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractFullClusterRestartTestCase extends ESRestTestCase {

    private final boolean runningAgainstOldCluster = Booleans.parseBoolean(System.getProperty("tests.is_old_cluster"));

    @Before
    public void init() throws IOException {
        assertThat("we don't need this branch if we aren't compatible with 6.0",
                Version.CURRENT.minimumIndexCompatibilityVersion().onOrBefore(Version.V_6_0_0), equalTo(true));
        if (isRunningAgainstOldCluster() && getOldClusterVersion().before(Version.V_7_0_0)) {
            XContentBuilder template = jsonBuilder();
            template.startObject();
            {
                template.field("index_patterns", "*");
                template.field("order", "0");
                template.startObject("settings");
                template.field("number_of_shards", 5);
                template.endObject();
            }
            template.endObject();
            Request createTemplate = new Request("PUT", "/_template/template");
            createTemplate.setJsonEntity(Strings.toString(template));
            client().performRequest(createTemplate);
        }
    }

    public final boolean isRunningAgainstOldCluster() {
        return runningAgainstOldCluster;
    }

    private final Version oldClusterVersion = Version.fromString(System.getProperty("tests.old_cluster_version"));

    public final Version getOldClusterVersion() {
        return oldClusterVersion;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveSnapshotsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveReposUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveTemplatesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveClusterSettings() {
        return true;
    }

    @Override
    protected boolean preserveRollupJobsUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveILMPoliciesUponCompletion() {
        return true;
    }
}
