/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.idp.saml.test.IdpSamlTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;

public class DeleteSamlServiceProviderResponseTests extends IdpSamlTestCase {

    public void testToXContent() throws Exception {
        final String docId = randomAlphaOfLength(24);
        final long seqNo = randomNonNegativeLong();
        final long primaryTerm = randomNonNegativeLong();
        final String entityId = randomAlphaOfLengthBetween(4, 128);
        final DeleteSamlServiceProviderResponse response = new DeleteSamlServiceProviderResponse(docId, seqNo, primaryTerm, entityId);
        final String xContent = Strings.toString(response);
        final Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), xContent, randomBoolean());
        assertThat(map.keySet(), containsInAnyOrder("document", "service_provider"));

        assertThat(map.get("document"), instanceOf(Map.class));
        assertThat(((Map<?, ?>) map.get("document")).keySet(), containsInAnyOrder("_id", "_seq_no", "_primary_term"));

        assertThat(map.get("service_provider"), instanceOf(Map.class));
        assertThat(((Map<?, ?>) map.get("service_provider")).keySet(), containsInAnyOrder("entity_id"));
    }

}
