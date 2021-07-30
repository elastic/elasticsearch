/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.test.eql;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.ql.util.StringUtils.EMPTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class EqlRestValidationTestCase extends RemoteClusterAwareEqlRestTestCase {

    private static final String indexName = "test_eql";
    protected static final String[] existentIndexWithWildcard = new String[] {indexName + ",inexistent*", indexName + "*,inexistent*",
        "inexistent*," + indexName};
    private static final String[] existentIndexWithoutWildcard = new String[] {indexName + ",inexistent", "inexistent," + indexName};
    protected static final String[] inexistentIndexNameWithWildcard = new String[] {"inexistent*", "inexistent1*,inexistent2*"};
    protected static final String[] inexistentIndexNameWithoutWildcard = new String[] {"inexistent", "inexistent1,inexistent2"};

    @Before
    public void prepareIndices() throws IOException {
        if (provisioningClient().performRequest(new Request("HEAD", "/" + indexName)).getStatusLine().getStatusCode() == 404) {
            createIndex(indexName, (String) null);
        }

        Object[] fieldsAndValues = new Object[] {"event_type", "my_event", "@timestamp", "2020-10-08T12:35:48Z", "val", 0};
        XContentBuilder document = jsonBuilder().startObject();
        for (int i = 0; i < fieldsAndValues.length; i += 2) {
            document.field((String) fieldsAndValues[i], fieldsAndValues[i + 1]);
        }
        document.endObject();
        final Request request = new Request("POST", "/" + indexName + "/_doc/" + 0);
        request.setJsonEntity(Strings.toString(document));
        assertOK(provisioningClient().performRequest(request));

        assertOK(provisioningAdminClient().performRequest(new Request("POST", "/" + indexName + "/_refresh")));
    }

    protected abstract String getInexistentIndexErrorMessage();

    protected abstract void assertErrorMessageWhenAllowNoIndicesIsFalse(String reqParameter) throws IOException;

    public void testDefaultIndicesOptions() throws IOException {
        assertErrorMessages(inexistentIndexNameWithWildcard, EMPTY, getInexistentIndexErrorMessage());
        assertErrorMessages(inexistentIndexNameWithoutWildcard, EMPTY, getInexistentIndexErrorMessage());
        assertValidRequestOnIndices(existentIndexWithWildcard, EMPTY);
        assertValidRequestOnIndices(existentIndexWithoutWildcard, EMPTY);
    }

    public void testAllowNoIndicesOption() throws IOException {
        boolean allowNoIndices = randomBoolean();
        boolean setAllowNoIndices = randomBoolean();
        boolean isAllowNoIndices = allowNoIndices || setAllowNoIndices == false;
        String reqParameter = setAllowNoIndices ? "?allow_no_indices=" + allowNoIndices : EMPTY;

        if (isAllowNoIndices) {
            assertErrorMessages(inexistentIndexNameWithWildcard, reqParameter, getInexistentIndexErrorMessage());
            assertErrorMessages(inexistentIndexNameWithoutWildcard, reqParameter, getInexistentIndexErrorMessage());
            assertValidRequestOnIndices(existentIndexWithWildcard, reqParameter);
            assertValidRequestOnIndices(existentIndexWithoutWildcard, reqParameter);
        } else {
            assertValidRequestOnIndices(existentIndexWithoutWildcard, reqParameter);
            assertErrorMessageWhenAllowNoIndicesIsFalse(reqParameter);
        }
    }

    protected void assertErrorMessages(String[] indices, String reqParameter, String errorMessage) throws IOException {
        for (String indexName : indices) {
            assertErrorMessage(indexName, reqParameter, errorMessage + "[" + indexPattern(indexName) + "]");
        }
    }

    protected void assertErrorMessage(String indexName, String reqParameter, String errorMessage) throws IOException {
        final Request request = createRequest(indexName, reqParameter);
        ResponseException exc = expectThrows(ResponseException.class, () ->  client().performRequest(request));

        assertThat(exc.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(exc.getMessage(), containsString(errorMessage));
    }

    private Request createRequest(String indexName, String reqParameter) throws IOException {
        final Request request = new Request("POST", "/" + indexPattern(indexName) + "/_eql/search" + reqParameter);
        request.setJsonEntity(Strings.toString(JsonXContent.contentBuilder()
            .startObject()
            .field("event_category_field", "event_type")
            .field("query", "my_event where true")
            .endObject()));

        return request;
    }

    private void assertValidRequestOnIndices(String[] indices, String reqParameter) throws IOException {
        for (String indexName : indices) {
            final Request request = createRequest(indexName, reqParameter);
            Response response = client().performRequest(request);
            assertOK(response);
        }
    }

    protected String indexPattern(String index) {
        return index;
    }
}
