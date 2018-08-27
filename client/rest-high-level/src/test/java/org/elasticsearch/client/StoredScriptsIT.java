package org.elasticsearch.client;/*
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

import org.apache.http.util.EntityUtils;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.StoredScriptSource;

import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class StoredScriptsIT extends ESRestHighLevelClientTestCase {

    final String id = "calculate-score";

    public void testGetStoredScript() throws Exception {
        final StoredScriptSource scriptSource =
            new StoredScriptSource("painless",
                "Math.log(_score * 2) + params.my_modifier",
            Collections.singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType()));

        final String script = Strings.toString(scriptSource.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));
        // TODO: change to HighLevel PutStoredScriptRequest when it will be ready
        // so far - using low-level REST API
        Request putRequest = new Request("PUT", "/_scripts/calculate-score");
        putRequest.setJsonEntity("{\"script\":" + script + "}");
        Response putResponse = adminClient().performRequest(putRequest);
        assertEquals("{\"acknowledged\":true}", EntityUtils.toString(putResponse.getEntity()));

        GetStoredScriptRequest getRequest = new GetStoredScriptRequest("calculate-score");
        getRequest.masterNodeTimeout("50s");

        GetStoredScriptResponse getResponse = execute(getRequest, highLevelClient()::getScript,
            highLevelClient()::getScriptAsync);

        assertThat(getResponse.getSource(), equalTo(scriptSource));
    }

    public void testDeleteStoredScript() throws Exception {
        final StoredScriptSource scriptSource =
            new StoredScriptSource("painless",
                "Math.log(_score * 2) + params.my_modifier",
                Collections.singletonMap(Script.CONTENT_TYPE_OPTION, XContentType.JSON.mediaType()));

        final String script = Strings.toString(scriptSource.toXContent(jsonBuilder(), ToXContent.EMPTY_PARAMS));
        // TODO: change to HighLevel PutStoredScriptRequest when it will be ready
        // so far - using low-level REST API
        Request putRequest = new Request("PUT", "/_scripts/" + id);
        putRequest.setJsonEntity("{\"script\":" + script + "}");
        Response putResponse = adminClient().performRequest(putRequest);
        assertEquals("{\"acknowledged\":true}", EntityUtils.toString(putResponse.getEntity()));

        DeleteStoredScriptRequest deleteRequest = new DeleteStoredScriptRequest(id);
        deleteRequest.masterNodeTimeout("50s");
        deleteRequest.timeout("50s");

        AcknowledgedResponse deleteResponse = execute(deleteRequest, highLevelClient()::deleteScript,
            highLevelClient()::deleteScriptAsync);

        assertThat(deleteResponse.isAcknowledged(), equalTo(true));

        GetStoredScriptRequest getRequest = new GetStoredScriptRequest(id);

        final ElasticsearchStatusException statusException = expectThrows(ElasticsearchStatusException.class,
            () -> execute(getRequest, highLevelClient()::getScript,
                highLevelClient()::getScriptAsync));
        assertThat(statusException.status(), equalTo(RestStatus.NOT_FOUND));
    }
}
