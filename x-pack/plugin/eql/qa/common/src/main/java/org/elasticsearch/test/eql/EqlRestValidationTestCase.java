/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.test.eql;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.ql.util.StringUtils.EMPTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class EqlRestValidationTestCase extends ESRestTestCase {

    // TODO: handle for existent indices the patterns "test,inexistent", "inexistent,test" that seem to work atm as is
    private static final String[] existentIndexName = new String[] {"test,inexistent*", "test*,inexistent*", "inexistent*,test"};
    private static final String[] inexistentIndexName = new String[] {"inexistent", "inexistent*", "inexistent1*,inexistent2*"};

    @Before
    public void prepareIndices() throws IOException {
        createIndex("test", Settings.EMPTY);

        Object[] fieldsAndValues = new Object[] {"event_type", "my_event", "@timestamp", "2020-10-08T12:35:48Z", "val", 0};
        XContentBuilder document = jsonBuilder().startObject();
        for (int i = 0; i < fieldsAndValues.length; i += 2) {
            document.field((String) fieldsAndValues[i], fieldsAndValues[i + 1]);
        }
        document.endObject();
        final Request request = new Request("POST", "/test/_doc/" + 0);
        request.setJsonEntity(Strings.toString(document));
        assertOK(client().performRequest(request));

        assertOK(adminClient().performRequest(new Request("POST", "/test/_refresh")));
    }

    public void testDefaultIndicesOptions() throws IOException {
        String message = "\"root_cause\":[{\"type\":\"verification_exception\",\"reason\":\"Found 1 problem\\nline -1:-1: Unknown index";
        assertErrorMessageOnInexistentIndices(EMPTY, true, message, EMPTY);
        assertErrorMessageOnExistentIndices("?allow_no_indices=false", false, message, EMPTY);
        assertValidRequestOnExistentIndices(EMPTY);
    }

    public void testAllowNoIndicesOption() throws IOException {
        boolean allowNoIndices = randomBoolean();
        boolean setAllowNoIndices = randomBoolean();
        boolean isAllowNoIndices = allowNoIndices || setAllowNoIndices == false;

        String allowNoIndicesTrueMessage = "\"root_cause\":[{\"type\":\"verification_exception\",\"reason\":"
            + "\"Found 1 problem\\nline -1:-1: Unknown index";
        String allowNoIndicesFalseMessage = "\"root_cause\":[{\"type\":\"index_not_found_exception\",\"reason\":\"no such index";
        String reqParameter = setAllowNoIndices ? "?allow_no_indices=" + allowNoIndices : EMPTY;

        assertErrorMessageOnInexistentIndices(reqParameter, isAllowNoIndices, allowNoIndicesTrueMessage, allowNoIndicesFalseMessage);
        if (isAllowNoIndices) {
            assertValidRequestOnExistentIndices(reqParameter);
        }
    }

    private void assertErrorMessageOnExistentIndices(String reqParameter, boolean isAllowNoIndices, String allowNoIndicesTrueMessage,
        String allowNoIndicesFalseMessage) throws IOException {
        assertErrorMessages(existentIndexName, reqParameter, isAllowNoIndices, allowNoIndicesTrueMessage, allowNoIndicesFalseMessage);
    }

    private void assertErrorMessageOnInexistentIndices(String reqParameter, boolean isAllowNoIndices, String allowNoIndicesTrueMessage,
        String allowNoIndicesFalseMessage) throws IOException {
        assertErrorMessages(inexistentIndexName, reqParameter, isAllowNoIndices, allowNoIndicesTrueMessage, allowNoIndicesFalseMessage);
    }

    private void assertErrorMessages(String[] indices, String reqParameter, boolean isAllowNoIndices, String allowNoIndicesTrueMessage,
        String allowNoIndicesFalseMessage) throws IOException {
        for (String indexName : indices) {
            final Request request = createRequest(indexName, reqParameter);
            ResponseException exc = expectThrows(ResponseException.class, () ->  client().performRequest(request));

            assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(isAllowNoIndices ? 400 : 404));
            // TODO add the index name to the message to be checked. Waiting on https://github.com/elastic/elasticsearch/issues/63529
            assertThat(exc.getMessage(), containsString(isAllowNoIndices ? allowNoIndicesTrueMessage : allowNoIndicesFalseMessage));
        }
    }

    private Request createRequest(String indexName, String reqParameter) throws IOException {
        final Request request = new Request("POST", "/" + indexName + "/_eql/search" + reqParameter);
        request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder()
            .startObject()
            .field("event_category_field", "event_type")
            .field("query", "my_event where true")
            .endObject()));

        return request;
    }

    private void assertValidRequestOnExistentIndices(String reqParameter) throws IOException {
        for (String indexName : existentIndexName) {
            final Request request = createRequest(indexName, reqParameter);
            Response response = client().performRequest(request);
            assertOK(response);
        }
    }
}
