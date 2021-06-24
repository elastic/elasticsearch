/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.enrollment;

import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.security.NodeEnrollmentResponse;

import java.util.List;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class EnrollmentIT extends ESRestHighLevelClientTestCase {

    public void testEnrollNode() throws Exception {
        final NodeEnrollmentResponse nodeEnrollmentResponse =
            execute(highLevelClient().security()::enrollNode, highLevelClient().security()::enrollNodeAsync, RequestOptions.DEFAULT);
        assertThat(nodeEnrollmentResponse, notNullValue());
        assertThat(nodeEnrollmentResponse.getHttpCaKey(), endsWith("ECAwGGoA=="));
        assertThat(nodeEnrollmentResponse.getHttpCaCert(), endsWith("ECAwGGoA=="));
        assertThat(nodeEnrollmentResponse.getTransportKey(), endsWith("fSI09on8AgMBhqA="));
        assertThat(nodeEnrollmentResponse.getTransportCert(), endsWith("fSI09on8AgMBhqA="));
        List<String> nodesAddresses = nodeEnrollmentResponse.getNodesAddresses();
        assertThat(nodesAddresses.size(), equalTo(1));
    }
}
