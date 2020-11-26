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

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.junit.Before;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeThat;

/**
 * Basic tests for simple xpack functionality that are only run if the
 * cluster is the on the default distribution.
 */
public class XPackIT extends AbstractRollingTestCase {
    @Before
    public void skipIfNotXPack() {
        assumeThat("test is only supported if the distribution contains xpack",
                System.getProperty("tests.distribution"), equalTo("default"));
        assumeThat("running this on the unupgraded cluster would change its state and it wouldn't work prior to 6.3 anyway",
                CLUSTER_TYPE, equalTo(ClusterType.UPGRADED));
        /*
         * *Mostly* we want this for when we're upgrading from pre-6.3's
         * zip distribution which doesn't contain xpack to post 6.3's zip
         * distribution which *does* contain xpack. But we'll also run it
         * on all upgrades for completeness's sake.
         */
    }

    /**
     * Test a basic feature (SQL) which doesn't require any trial license.
     * Note that the test methods on this class can run in any order so we
     * <strong>might</strong> have already installed a trial license.
     */
    public void testBasicFeature() throws IOException {
        Request bulk = new Request("POST", "/sql_test/_bulk");
        bulk.setJsonEntity(
              "{\"index\":{}}\n"
            + "{\"f\": \"1\"}\n"
            + "{\"index\":{}}\n"
            + "{\"f\": \"2\"}\n");
        bulk.addParameter("refresh", "true");
        client().performRequest(bulk);

        Request sql = new Request("POST", "/_sql");
        sql.setJsonEntity("{\"query\": \"SELECT * FROM sql_test WHERE f > 1 ORDER BY f ASC\"}");
        String response = EntityUtils.toString(client().performRequest(sql).getEntity());
        assertEquals("{\"columns\":[{\"name\":\"f\",\"type\":\"text\"}],\"rows\":[[\"2\"]]}", response);
    }

    /**
     * Test creating a trial license and using it. This is interesting because
     * our other tests test cover starting a new cluster with the default
     * distribution and enabling the trial license but this test is the only
     * one that can upgrade from the oss distribution to the default
     * distribution with xpack and the create a trial license. We don't
     * <strong>do</strong> a lot with the trial license because for the most
     * part those things are tested elsewhere, off in xpack. But we do use the
     * trial license a little bit to make sure that it works.
     */
    public void testTrialLicense() throws IOException {
        Request startTrial = new Request("POST", "/_license/start_trial");
        startTrial.addParameter("acknowledge", "true");
        client().performRequest(startTrial);

        String noJobs = EntityUtils.toString(
            client().performRequest(new Request("GET", "/_ml/anomaly_detectors")).getEntity());
        assertEquals("{\"count\":0,\"jobs\":[]}", noJobs);

        Request createJob = new Request("PUT", "/_ml/anomaly_detectors/test_job");
        createJob.setJsonEntity(
                  "{\n"
                + "  \"analysis_config\" : {\n"
                + "    \"bucket_span\": \"10m\",\n"
                + "    \"detectors\": [\n"
                + "      {\n"
                + "        \"function\": \"sum\",\n"
                + "        \"field_name\": \"total\"\n"
                + "      }\n"
                + "    ]\n"
                + "  },\n"
                + "  \"data_description\": {\n"
                + "    \"time_field\": \"timestamp\",\n"
                + "    \"time_format\": \"epoch_ms\"\n"
                + "  }\n"
                + "}\n");
        client().performRequest(createJob);
    }
}
