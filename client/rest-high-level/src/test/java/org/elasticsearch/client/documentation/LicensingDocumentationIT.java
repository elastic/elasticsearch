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

package org.elasticsearch.client.documentation;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.protocol.xpack.license.LicensesStatus;
import org.elasticsearch.protocol.xpack.license.PutLicenseRequest;
import org.elasticsearch.protocol.xpack.license.PutLicenseResponse;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

/**
 * Documentation for Licensing APIs in the high level java client.
 * Code wrapped in {@code tag} and {@code end} tags is included in the docs.
 */
@AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/32580")
public class LicensingDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testPutLicense() throws Exception {
        RestHighLevelClient client = highLevelClient();
        String license = "{\"license\": {\"uid\":\"893361dc-9749-4997-93cb-802e3d7fa4a8\",\"type\":\"gold\"," +
            "\"issue_date_in_millis\":1411948800000,\"expiry_date_in_millis\":1914278399999,\"max_nodes\":1,\"issued_to\":\"issued_to\"," +
            "\"issuer\":\"issuer\",\"signature\":\"AAAAAgAAAA3U8+YmnvwC+CWsV/mRAAABmC9ZN0hjZDBGYnVyRXpCOW5Bb3FjZDAxOWpSbTVoMVZwUzRxVk1PSm" +
            "kxakxZdW5IMlhlTHNoN1N2MXMvRFk4d3JTZEx3R3RRZ0pzU3lobWJKZnQvSEFva0ppTHBkWkprZWZSQi9iNmRQNkw1SlpLN0lDalZCS095MXRGN1lIZlpYcVVTTn" +
            "FrcTE2dzhJZmZrdFQrN3JQeGwxb0U0MXZ0dDJHSERiZTVLOHNzSDByWnpoZEphZHBEZjUrTVBxRENNSXNsWWJjZllaODdzVmEzUjNiWktNWGM5TUhQV2plaUo4Q1" +
            "JOUml4MXNuL0pSOEhQaVB2azhmUk9QVzhFeTFoM1Q0RnJXSG53MWk2K055c28zSmRnVkF1b2JSQkFLV2VXUmVHNDZ2R3o2VE1qbVNQS2lxOHN5bUErZlNIWkZSVm" +
            "ZIWEtaSU9wTTJENDVvT1NCYklacUYyK2FwRW9xa0t6dldMbmMzSGtQc3FWOTgzZ3ZUcXMvQkt2RUZwMFJnZzlvL2d2bDRWUzh6UG5pdENGWFRreXNKNkE9PQAAAQ" +
            "Be8GfzDm6T537Iuuvjetb3xK5dvg0K5NQapv+rczWcQFxgCuzbF8plkgetP1aAGZP4uRESDQPMlOCsx4d0UqqAm9f7GbBQ3l93P+PogInPFeEH9NvOmaAQovmxVM" +
            "9SE6DsDqlX4cXSO+bgWpXPTd2LmpoQc1fXd6BZ8GeuyYpVHVKp9hVU0tAYjw6HzYOE7+zuO1oJYOxElqy66AnIfkvHrvni+flym3tE7tDTgsDRaz7W3iBhaqiSnt" +
            "EqabEkvHdPHQdSR99XGaEvnHO1paK01/35iZF6OXHsF7CCj+558GRXiVxzueOe7TsGSSt8g7YjZwV9bRCyU7oB4B/nidgI\"}}";
        {
            //tag::put-license-execute
            PutLicenseRequest request = new PutLicenseRequest();
            request.setLicenseDefinition(license);  // <1>
            request.setAcknowledge(false);          // <2>

            PutLicenseResponse response = client.license().putLicense(request, RequestOptions.DEFAULT);
            //end::put-license-execute

            //tag::put-license-response
            LicensesStatus status = response.status();                  // <1>
            assertEquals(status, LicensesStatus.VALID);                 // <2>
            boolean acknowledged = response.isAcknowledged();           // <3>
            String acknowledgeHeader = response.acknowledgeHeader();    // <4>
            Map<String, String[]> acknowledgeMessages = response.acknowledgeMessages();  // <5>
            //end::put-license-response

            assertFalse(acknowledged); // Should fail because we are trying to downgrade from platinum trial to gold
            assertThat(acknowledgeHeader, startsWith("This license update requires acknowledgement."));
            assertThat(acknowledgeMessages.keySet(), not(hasSize(0)));
        }
        {
            PutLicenseRequest request = new PutLicenseRequest();
            // tag::put-license-execute-listener
            ActionListener<PutLicenseResponse> listener = new ActionListener<PutLicenseResponse>() {
                @Override
                public void onResponse(PutLicenseResponse indexResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-license-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-license-execute-async
            client.license().putLicenseAsync(
                    request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-license-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
}
