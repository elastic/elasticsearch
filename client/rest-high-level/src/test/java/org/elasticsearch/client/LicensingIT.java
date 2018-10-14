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
package org.elasticsearch.client;

import org.elasticsearch.client.license.GetLicenseRequest;
import org.elasticsearch.client.license.GetLicenseResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;

public class LicensingIT extends ESRestHighLevelClientTestCase {
    public void testGetLicense() throws Exception {
        final GetLicenseRequest request = new GetLicenseRequest();
        final GetLicenseResponse response = highLevelClient().license().getLicense(request, RequestOptions.DEFAULT);
        final String licenseDefinition = response.getLicenseDefinition();
        assertThat(licenseDefinition, notNullValue());

        final XContentParser parser = createParser(JsonXContent.jsonXContent, licenseDefinition);
        final Map<String, Object> map = parser.map();
        assertThat(map.containsKey("license"), equalTo(true));

        @SuppressWarnings("unchecked")
        final Map<String, Object> license = (Map<String, Object>) map.get("license");
        assertThat(license.get("status"), equalTo("active"));
        assertThat(license.get("type"), equalTo("trial"));
    }

    public void testPutLicense() throws Exception {
        // TODO
    }

    public void testDeleteLicense() throws Exception {
        // TODO
    }
}
