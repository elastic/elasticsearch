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

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.textstructure.FindStructureRequest;
import org.elasticsearch.client.textstructure.FindStructureResponse;
import org.elasticsearch.client.textstructure.structurefinder.TextStructure;

public class TextStructureClientDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testFindStructure() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Path anInterestingFile = createTempFile();
        String contents = "{\"logger\":\"controller\",\"timestamp\":1478261151445,\"level\":\"INFO\"," +
                "\"pid\":42,\"thread\":\"0x7fff7d2a8000\",\"message\":\"message 1\",\"class\":\"ml\"," +
                "\"method\":\"core::SomeNoiseMaker\",\"file\":\"Noisemaker.cc\",\"line\":333}\n" +
            "{\"logger\":\"controller\",\"timestamp\":1478261151445," +
                "\"level\":\"INFO\",\"pid\":42,\"thread\":\"0x7fff7d2a8000\",\"message\":\"message 2\",\"class\":\"ml\"," +
                "\"method\":\"core::SomeNoiseMaker\",\"file\":\"Noisemaker.cc\",\"line\":333}\n";
        Files.write(anInterestingFile, Collections.singleton(contents), StandardCharsets.UTF_8);

        {
            // tag::find-structure-request
            FindStructureRequest request = new FindStructureRequest(); // <1>
            request.setSample(Files.readAllBytes(anInterestingFile)); // <2>
            // end::find-structure-request

            // tag::find-structure-request-options
            request.setLinesToSample(500); // <1>
            request.setExplain(true); // <2>
            // end::find-structure-request-options

            // tag::find-structure-execute
            FindStructureResponse response = client
                .textStructure()
                .findStructure(
                    request,
                    RequestOptions.DEFAULT
                );
            // end::find-structure-execute

            // tag::find-structure-response
            TextStructure structure = response.getFileStructure(); // <1>
            // end::find-structure-response
            assertEquals(2, structure.getNumLinesAnalyzed());
        }
        {
            // tag::find-structure-execute-listener
            ActionListener<FindStructureResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(FindStructureResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::find-structure-execute-listener
            FindStructureRequest request = new FindStructureRequest();
            request.setSample(Files.readAllBytes(anInterestingFile));

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::find-structure-execute-async
            client
                .textStructure()
                .findStructureAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::find-structure-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

}
