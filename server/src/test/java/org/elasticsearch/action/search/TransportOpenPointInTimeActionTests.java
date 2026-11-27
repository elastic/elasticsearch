/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteTransportException;

import java.net.InetAddress;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class TransportOpenPointInTimeActionTests extends ESTestCase {

    public void testAnUnreachableProjectFailsAStrictOpen() {
        IllegalArgumentException down = new IllegalArgumentException("link down");
        RemoteTransportException wrapped = new RemoteTransportException(
            "node",
            new TransportAddress(InetAddress.getLoopbackAddress(), 9300),
            "indices:admin/resolve/index",
            down
        );
        Exception failure = TransportOpenPointInTimeAction.failureForUnreachableProjects(false, Map.of("P1", wrapped));
        assertThat(failure, instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure).status(), equalTo(RestStatus.BAD_REQUEST));
        assertNull(failure.getCause());
        ElasticsearchException[] rootCauses = ElasticsearchException.guessRootCauses(failure);
        assertThat(rootCauses[0].getMessage(), containsString("[P1]"));
        assertThat(rootCauses[0].getMessage(), containsString("link down"));
        assertEquals(0, failure.getSuppressed().length);
        assertEquals(0, down.getSuppressed().length);
    }

    public void testPartialResultsLeaveAnUnreachableProjectToTheOpen() {
        assertNull(
            TransportOpenPointInTimeAction.failureForUnreachableProjects(true, Map.of("P1", new IllegalStateException("link down")))
        );
    }

    public void testACompleteResolutionDoesNotFail() {
        assertNull(TransportOpenPointInTimeAction.failureForUnreachableProjects(randomBoolean(), Map.of()));
    }

    public void testLaterProjectsAreSuppressedOnTheFirstFailure() {
        EsRejectedExecutionException first = new EsRejectedExecutionException("first-link-down");
        IllegalArgumentException second = new IllegalArgumentException("second-link-down");
        Map<String, Exception> remoteFailures = new LinkedHashMap<>();
        remoteFailures.put("b-cluster", second);
        remoteFailures.put("a-cluster", first);
        Exception failure = TransportOpenPointInTimeAction.failureForUnreachableProjects(false, remoteFailures);
        assertThat(failure, instanceOf(ElasticsearchStatusException.class));
        assertThat(((ElasticsearchStatusException) failure).status(), equalTo(RestStatus.TOO_MANY_REQUESTS));
        assertNull(failure.getCause());
        assertThat(failure.getMessage(), containsString("[a-cluster]"));
        assertThat(failure.getMessage(), containsString("first-link-down"));
        assertThat(failure.getSuppressed(), arrayContaining(instanceOf(ElasticsearchStatusException.class)));
        ElasticsearchStatusException suppressed = (ElasticsearchStatusException) failure.getSuppressed()[0];
        assertThat(suppressed.status(), equalTo(RestStatus.BAD_REQUEST));
        assertThat(suppressed.getMessage(), containsString("[b-cluster]"));
        assertThat(suppressed.getMessage(), containsString("second-link-down"));
        assertNull(suppressed.getCause());
        ElasticsearchException[] rootCauses = ElasticsearchException.guessRootCauses(failure);
        assertThat(rootCauses[0].getMessage(), containsString("[a-cluster]"));
        assertThat(rootCauses[0].getMessage(), containsString("first-link-down"));
        assertEquals(0, first.getSuppressed().length);
        assertEquals(0, second.getSuppressed().length);
    }
}
