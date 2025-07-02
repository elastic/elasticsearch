/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.streams;

import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.StreamsMetadata;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamsPermissionsUtilsTests extends ESTestCase {

    private StreamsPermissionsUtils utils;
    private ProjectMetadata projectMetadataMock;
    private StreamsMetadata streamsMetadataMock;

    @Before
    public void setup() {
        utils = StreamsPermissionsUtils.getInstance();
        projectMetadataMock = mock(ProjectMetadata.class);
        streamsMetadataMock = mock(StreamsMetadata.class);
        when(projectMetadataMock.custom(eq(StreamsMetadata.TYPE), any())).thenReturn(streamsMetadataMock);
    }

    public void testGetInstanceReturnsSingleton() {
        StreamsPermissionsUtils instance1 = StreamsPermissionsUtils.getInstance();
        StreamsPermissionsUtils instance2 = StreamsPermissionsUtils.getInstance();
        assertThat(instance1, sameInstance(instance2));
    }

    public void testStreamTypeIsEnabledReturnsTrueWhenLogsEnabled() {
        when(streamsMetadataMock.isLogsEnabled()).thenReturn(true);

        boolean result = utils.streamTypeIsEnabled(StreamTypes.LOGS, projectMetadataMock);
        assertTrue(result);
    }

    public void testStreamTypeIsEnabledReturnsFalseWhenLogsDisabled() {
        when(streamsMetadataMock.isLogsEnabled()).thenReturn(false);

        boolean result = utils.streamTypeIsEnabled(StreamTypes.LOGS, projectMetadataMock);
        assertFalse(result);
    }

    public void testIfRetrouteToSubstreamNotAllowedThrows() {
        when(streamsMetadataMock.isLogsEnabled()).thenReturn(true);

        Set<String> indexHistory = new HashSet<>(); // empty, so reroute not allowed
        String destination = StreamTypes.LOGS.getStreamName() + ".substream";

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> utils.throwIfRetrouteToSubstreamNotAllowed(projectMetadataMock, indexHistory, destination)
        );

        assertTrue(ex.getMessage().contains("Cannot reroute to substream"));
        assertTrue(ex.getMessage().contains(destination));
    }

    public void testThrowIfRetrouteToSubstreamNotAllowedDoesNotThrowWhenStreamTypeDisabled() {
        when(streamsMetadataMock.isLogsEnabled()).thenReturn(false);

        Set<String> indexHistory = Collections.emptySet();
        String destination = StreamTypes.LOGS.getStreamName() + ".substream";

        // Should not throw since stream type is disabled
        utils.throwIfRetrouteToSubstreamNotAllowed(projectMetadataMock, indexHistory, destination);
    }

    public void testThrowIfRetrouteToSubstreamNotAllowedDoesNotThrowWhenDestinationNotSubstream() {
        when(streamsMetadataMock.isLogsEnabled()).thenReturn(true);

        Set<String> indexHistory = Collections.emptySet();
        String destination = StreamTypes.LOGS.getStreamName(); // not a substream

        // Should not throw since destination is not a substream
        utils.throwIfRetrouteToSubstreamNotAllowed(projectMetadataMock, indexHistory, destination);
    }

    public void testThrowIfRetrouteToSubstreamNotAllowedDoesNotThrowWhenIndexHistoryContainsStream() {
        when(streamsMetadataMock.isLogsEnabled()).thenReturn(true);

        Set<String> indexHistory = new HashSet<>();
        indexHistory.add(StreamTypes.LOGS.getStreamName());
        String destination = StreamTypes.LOGS.getStreamName() + ".substream";

        // Should not throw since indexHistory contains the stream name
        utils.throwIfRetrouteToSubstreamNotAllowed(projectMetadataMock, indexHistory, destination);
    }
}
