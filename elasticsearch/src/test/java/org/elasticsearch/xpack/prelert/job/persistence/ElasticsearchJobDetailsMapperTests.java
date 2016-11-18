/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.action.get.GetRequestBuilder;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.ModelSizeStats;
import org.elasticsearch.xpack.prelert.job.results.ReservedFieldNames;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ElasticsearchJobDetailsMapperTests extends ESTestCase {
    private Client client;

    @Before
    public void setUpMocks() {
        client = Mockito.mock(Client.class);
    }

    public void testMap_GivenJobSourceCannotBeParsed() {
        BytesArray source = new BytesArray("{ \"invalidKey\": true }");

        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(false);
        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        when(getRequestBuilder.get()).thenReturn(getResponse);
        when(client.prepareGet("prelertresults-foo", ModelSizeStats.TYPE.getPreferredName(), ModelSizeStats.TYPE.getPreferredName()))
        .thenReturn(getRequestBuilder);

        ElasticsearchJobDetailsMapper mapper = new ElasticsearchJobDetailsMapper(client, ParseFieldMatcher.STRICT);

        ESTestCase.expectThrows(IllegalArgumentException.class, () -> mapper.map(source));
    }

    public void testMap_GivenModelSizeStatsExists() throws Exception {
        ModelSizeStats.Builder modelSizeStats = new ModelSizeStats.Builder("foo");
        modelSizeStats.setModelBytes(42L);
        Date now = new Date();
        modelSizeStats.setTimestamp(now);

        Job originalJob = buildJobBuilder("foo").build();

        BytesReference source = originalJob.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).bytes();
        BytesReference modelSizeStatsSource = modelSizeStats.build().toXContent(
                XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).bytes();

        GetResponse getModelSizeResponse = mock(GetResponse.class);
        when(getModelSizeResponse.isExists()).thenReturn(true);
        when(getModelSizeResponse.getSourceAsBytesRef()).thenReturn(modelSizeStatsSource);
        GetRequestBuilder getModelSizeRequestBuilder = mock(GetRequestBuilder.class);
        when(getModelSizeRequestBuilder.get()).thenReturn(getModelSizeResponse);
        when(client.prepareGet("prelertresults-foo", ModelSizeStats.TYPE.getPreferredName(), ModelSizeStats.TYPE.getPreferredName()))
        .thenReturn(getModelSizeRequestBuilder);


        Map<String, Object> procTimeSource = new HashMap<>();
        procTimeSource.put(ReservedFieldNames.AVERAGE_PROCESSING_TIME_MS, 20.2);

        GetResponse getProcTimeResponse = mock(GetResponse.class);
        when(getProcTimeResponse.isExists()).thenReturn(true);
        when(getProcTimeResponse.getSource()).thenReturn(procTimeSource);
        GetRequestBuilder getProcTimeRequestBuilder = mock(GetRequestBuilder.class);
        when(getProcTimeRequestBuilder.get()).thenReturn(getProcTimeResponse);
        when(client.prepareGet("prelertresults-foo", ReservedFieldNames.BUCKET_PROCESSING_TIME_TYPE,
                ReservedFieldNames.AVERAGE_PROCESSING_TIME_MS))
        .thenReturn(getProcTimeRequestBuilder);


        ElasticsearchJobDetailsMapper mapper = new ElasticsearchJobDetailsMapper(client, ParseFieldMatcher.STRICT);

        Job mappedJob = mapper.map(source);

        assertEquals("foo", mappedJob.getId());
        assertEquals(42L, mappedJob.getModelSizeStats().getModelBytes());
        assertEquals(now, mappedJob.getModelSizeStats().getTimestamp());
        assertEquals(20.2, mappedJob.getAverageBucketProcessingTimeMs(), 0.0001);
    }

    public void testMap_GivenModelSizeStatsDoesNotExist() throws Exception {
        Job originalJob = buildJobBuilder("foo").build();

        BytesReference source = originalJob.toXContent(XContentFactory.jsonBuilder(), ToXContent.EMPTY_PARAMS).bytes();

        GetResponse getResponse = mock(GetResponse.class);
        when(getResponse.isExists()).thenReturn(false);
        GetRequestBuilder getRequestBuilder = mock(GetRequestBuilder.class);
        when(getRequestBuilder.get()).thenReturn(getResponse);
        when(client.prepareGet("prelertresults-foo", ModelSizeStats.TYPE.getPreferredName(), ModelSizeStats.TYPE.getPreferredName()))
        .thenReturn(getRequestBuilder);


        GetResponse getProcTimeResponse = mock(GetResponse.class);
        when(getProcTimeResponse.isExists()).thenReturn(false);
        GetRequestBuilder getProcTimeRequestBuilder = mock(GetRequestBuilder.class);
        when(getProcTimeRequestBuilder.get()).thenReturn(getProcTimeResponse);
        when(client.prepareGet("prelertresults-foo", ReservedFieldNames.BUCKET_PROCESSING_TIME_TYPE,
                ReservedFieldNames.AVERAGE_PROCESSING_TIME_MS))
        .thenReturn(getProcTimeRequestBuilder);

        ElasticsearchJobDetailsMapper mapper = new ElasticsearchJobDetailsMapper(client, ParseFieldMatcher.STRICT);

        Job mappedJob = mapper.map(source);

        assertEquals("foo", mappedJob.getId());
        assertNull(mappedJob.getModelSizeStats());
        assertNull(mappedJob.getAverageBucketProcessingTimeMs());
    }
}
