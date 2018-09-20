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

import org.elasticsearch.client.license.StartBasicRequest;
import org.elasticsearch.client.license.StartBasicResponse;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.empty;

public class LicensingIT extends ESRestHighLevelClientTestCase {

    public void testStartBasic() throws Exception {
        // we don't test the case where we successfully start a basic because the integ test cluster generates one on startup
        // and we don't have a good way to prevent that / work around it in this test project
        // case where we don't acknowledge basic license conditions
        {
            final StartBasicRequest request = new StartBasicRequest();
            final StartBasicResponse response = highLevelClient().license().startBasic(request, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), equalTo(false));
            assertThat(response.isBasicStarted(), equalTo(false));
            assertThat(response.getErrorMessage(), equalTo("Operation failed: Needs acknowledgement."));
            assertThat(response.getAcknowledgeMessage(),
                containsString("This license update requires acknowledgement. " +
                    "To acknowledge the license, please read the following messages and call /start_basic again"));
            assertNotEmptyAcknowledgeMessages(response);
        }
        // case where we acknowledge and the basic is started successfully
        {
            final StartBasicRequest request = new StartBasicRequest(true);
            final StartBasicResponse response = highLevelClient().license().startBasic(request, RequestOptions.DEFAULT);
            assertThat(response.isAcknowledged(), equalTo(true));
            assertThat(response.isBasicStarted(), equalTo(true));
            assertThat(response.getErrorMessage(), nullValue());
            assertThat(response.getAcknowledgeMessage(), nullValue());
            assertThat(response.getAcknowledgeMessages().size(), equalTo(0));
        }
    }

    private static void assertNotEmptyAcknowledgeMessages(StartBasicResponse response) {
        assertThat(response.getAcknowledgeMessages().entrySet(), not(empty()));
        for (Map.Entry<String, String[]> entry : response.getAcknowledgeMessages().entrySet()) {
            assertThat(entry.getKey(), not(isEmptyOrNullString()));
            final List<String> messages = Arrays.asList(entry.getValue());
            for (String message : messages) {
                assertThat(message, not(isEmptyOrNullString()));
            }
        }
    }
}
