/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.ingest.SamplingService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class GetSampleActionIT extends ESIntegTestCase {

    public void testGetSample() {
        GetSampleAction.Request request = new GetSampleAction.Request(ProjectId.DEFAULT, new String[] { "test_index" });
        GetSampleAction.Response response = client().execute(GetSampleAction.INSTANCE, request).actionGet();
        List<SamplingService.RawDocument> sample = response.getSamples();
        assertThat(sample, equalTo(List.of()));
    }
}
