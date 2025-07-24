/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessElectionStrategy;
import co.elastic.elasticsearch.stateless.cluster.coordination.StatelessLease;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthIndicatorDetails;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.health.ImpactArea;
import org.elasticsearch.health.node.HealthInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class BlobStoreHealthIndicatorTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testGreenFlow() throws IOException {
        ClusterService clusterService = mock(ClusterService.class);
        StatelessElectionStrategy electionStrategy = mock(StatelessElectionStrategy.class);
        AtomicLong nextTick = new AtomicLong();
        BlobStoreHealthIndicator indicator = new BlobStoreHealthIndicator(Settings.EMPTY, clusterService, electionStrategy, nextTick::get);
        indicator.start();
        ArgumentCaptor<ActionListener<Optional<StatelessLease>>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        long startTime = randomIntBetween(1, 100);
        nextTick.set(startTime);
        indicator.runCheck();
        verify(electionStrategy, times(1)).readLease(listenerCaptor.capture());
        long responseTime = startTime + randomIntBetween(1, 100);
        nextTick.set(responseTime);
        listenerCaptor.getValue()
            .onResponse(
                randomBoolean()
                    ? Optional.empty()
                    : Optional.of(new StatelessLease(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()))
            );
        long healthRequestTime = responseTime + randomIntBetween(1, 100);
        nextTick.set(healthRequestTime);
        HealthIndicatorResult result = indicator.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);
        assertThat(result.status(), is(HealthStatus.GREEN));
        assertThat(result.symptom(), startsWith("The cluster can access the blob store"));
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(getDuration(details, "time_since_last_check_started_millis"), equalTo(healthRequestTime - startTime));
        assertThat(getDuration(details, "time_since_last_update_millis"), equalTo(healthRequestTime - responseTime));
        assertThat(getDuration(details, "last_check_duration_millis"), equalTo(responseTime - startTime));
        assertThat(
            details.get("time_since_last_check_started"),
            equalTo(TimeValue.timeValueMillis(healthRequestTime - startTime).toString())
        );
        assertThat(details.get("time_since_last_update"), equalTo(TimeValue.timeValueMillis(healthRequestTime - responseTime).toString()));
        assertThat(details.get("last_check_duration"), equalTo(TimeValue.timeValueMillis(responseTime - startTime).toString()));
        assertThat(details.get("error_message"), is(nullValue()));
        assertThat(result.impacts(), empty());
        assertThat(result.diagnosisList(), empty());
        indicator.close();
    }

    @SuppressWarnings("unchecked")
    public void testRedFlow() throws IOException {
        ClusterService clusterService = mock(ClusterService.class);
        StatelessElectionStrategy electionStrategy = mock(StatelessElectionStrategy.class);

        AtomicLong nextTick = new AtomicLong();
        BlobStoreHealthIndicator indicator = new BlobStoreHealthIndicator(Settings.EMPTY, clusterService, electionStrategy, nextTick::get);
        indicator.start();
        ArgumentCaptor<ActionListener<Optional<StatelessLease>>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        long startTime = randomIntBetween(1, 100);
        nextTick.set(startTime);
        indicator.runCheck();
        long responseTime = startTime + randomIntBetween(1, 100);
        nextTick.set(responseTime);
        verify(electionStrategy, times(1)).readLease(listenerCaptor.capture());
        String errorMessage = randomAlphaOfLength(100);
        listenerCaptor.getValue().onFailure(new IllegalStateException(errorMessage));
        long healthRequestTime = responseTime + randomIntBetween(1, 100);
        nextTick.set(healthRequestTime);
        HealthIndicatorResult result = indicator.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);
        assertThat(result.status(), is(HealthStatus.RED));
        assertThat(result.symptom(), equalTo("The cluster failed to access the blob store."));
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(getDuration(details, "time_since_last_check_started_millis"), equalTo(healthRequestTime - startTime));
        assertThat(getDuration(details, "time_since_last_update_millis"), equalTo(healthRequestTime - responseTime));
        assertThat(getDuration(details, "last_check_duration_millis"), equalTo(responseTime - startTime));
        assertThat(details.get("error_message"), equalTo(errorMessage));
        assertThat(result.impacts().size(), is(1));
        assertThat(result.impacts().get(0).impactAreas().size(), is(4));
        assertThat(
            result.impacts().get(0).impactAreas(),
            containsInAnyOrder(ImpactArea.SEARCH, ImpactArea.INGEST, ImpactArea.DEPLOYMENT_MANAGEMENT, ImpactArea.BACKUP)
        );
        assertThat(result.diagnosisList().size(), is(1));
        assertThat(result.diagnosisList().get(0).definition().id(), equalTo("blob_store_inaccessible"));
        indicator.close();
    }

    @SuppressWarnings("unchecked")
    public void testRedDueToTimeout() throws Exception {
        ClusterService clusterService = mock(ClusterService.class);
        StatelessElectionStrategy electionStrategy = mock(StatelessElectionStrategy.class);
        AtomicLong nextTick = new AtomicLong();
        BlobStoreHealthIndicator indicator = new BlobStoreHealthIndicator(Settings.EMPTY, clusterService, electionStrategy, nextTick::get);
        indicator.start();
        ArgumentCaptor<ActionListener<Optional<StatelessLease>>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        long startTime = randomIntBetween(1, 100);
        nextTick.set(startTime);
        indicator.runCheck();
        verify(electionStrategy, times(1)).readLease(listenerCaptor.capture());
        // Fake that we exceeded the timeout
        long responseTime = startTime + indicator.getTimeout().millis() + randomIntBetween(1, 100);
        nextTick.set(responseTime);
        listenerCaptor.getValue()
            .onResponse(
                randomBoolean()
                    ? Optional.empty()
                    : Optional.of(new StatelessLease(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()))
            );
        long healthRequestTime = responseTime + randomIntBetween(1, 100);
        nextTick.set(healthRequestTime);
        HealthIndicatorResult result = indicator.calculate(true, HealthInfo.EMPTY_HEALTH_INFO);
        assertThat(result.status(), is(HealthStatus.RED));
        assertThat(result.symptom(), equalTo("The cluster failed to access the blob store."));
        Map<String, Object> details = xContentToMap(result.details());
        assertThat(((String) details.get("error_message")), startsWith("Reading from the blob store took"));
        assertThat(getDuration(details, "time_since_last_check_started_millis"), equalTo(healthRequestTime - startTime));
        assertThat(getDuration(details, "time_since_last_update_millis"), equalTo(healthRequestTime - responseTime));
        assertThat(getDuration(details, "last_check_duration_millis"), equalTo(responseTime - startTime));
        assertThat(result.diagnosisList().get(0).definition().id(), equalTo("blob_store_inaccessible"));
        indicator.close();
    }

    @SuppressWarnings("unchecked")
    public void testOneInstanceRunning() {
        ClusterService clusterService = mock(ClusterService.class);
        StatelessElectionStrategy electionStrategy = mock(StatelessElectionStrategy.class);
        BlobStoreHealthIndicator indicator = new BlobStoreHealthIndicator(
            Settings.EMPTY,
            clusterService,
            electionStrategy,
            () -> Clock.systemUTC().millis()
        );
        indicator.start();
        ArgumentCaptor<ActionListener<Optional<StatelessLease>>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        indicator.runCheck();
        indicator.runCheck();
        verify(electionStrategy, times(1)).readLease(listenerCaptor.capture());
        listenerCaptor.getValue()
            .onResponse(
                randomBoolean()
                    ? Optional.empty()
                    : Optional.of(new StatelessLease(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()))
            );
        assertThat(indicator.getInProgress(), is(false));
        indicator.close();
    }

    public void testGreenDueToInitialization() {
        long now = Clock.systemUTC().millis();
        BlobStoreHealthIndicator indicator = new BlobStoreHealthIndicator(
            Settings.EMPTY,
            mock(ClusterService.class),
            mock(StatelessElectionStrategy.class),
            () -> Clock.systemUTC().millis()
        );
        indicator.start();
        HealthIndicatorResult result = indicator.createHealthIndicatorResult(true, null, null, now);
        assertThat(result.status(), is(HealthStatus.GREEN));
        assertThat(result.symptom(), startsWith("The cluster is initialising"));
        assertThat(result.details(), is(HealthIndicatorDetails.EMPTY));
        assertThat(result.impacts(), empty());
        assertThat(result.diagnosisList(), empty());
        indicator.close();
    }

    public void testYellowDueToStaleResult() {
        long now = Clock.systemUTC().millis();
        long outdatedCheckTime = now - TimeValue.timeValueHours(1).millis();
        long finishTime = outdatedCheckTime + randomIntBetween(50, 1000);
        BlobStoreHealthIndicator indicator = new BlobStoreHealthIndicator(
            Settings.EMPTY,
            mock(ClusterService.class),
            mock(StatelessElectionStrategy.class),
            () -> Clock.systemUTC().millis()
        );
        indicator.start();
        // verbose: false is important because we are not running the health check so some fields needed for the details will not be set
        HealthIndicatorResult result = indicator.createHealthIndicatorResult(
            false,
            new BlobStoreHealthIndicator.HealthCheckResult(outdatedCheckTime, finishTime, null),
            outdatedCheckTime,
            now
        );
        assertThat(result.status(), is(HealthStatus.YELLOW));
        assertThat(
            result.symptom(),
            startsWith("It is uncertain that the cluster can access the blob store, last successful check started")
        );
        assertThat(result.impacts(), empty());
        assertThat(result.diagnosisList().size(), is(1));
        assertThat(result.diagnosisList().get(0).definition().id(), equalTo("stale_status"));
        indicator.close();
    }

    public void testLifecycle() {
        ClusterService clusterService = mock(ClusterService.class);
        StatelessElectionStrategy electionStrategy = mock(StatelessElectionStrategy.class);
        doAnswer(invocation -> {
            ActionListener<Optional<StatelessLease>> listener = invocation.getArgument(0);
            listener.onResponse(Optional.empty());
            return null;
        }).when(electionStrategy).readLease(any());

        AtomicLong nextTick = new AtomicLong();
        BlobStoreHealthIndicator indicator = new BlobStoreHealthIndicator(Settings.EMPTY, clusterService, electionStrategy, nextTick::get);

        long startTime = randomIntBetween(1, 100);
        indicator.runCheck();
        verify(electionStrategy, never()).readLease(any());

        indicator.start();
        indicator.stop();
        if (randomBoolean()) {
            indicator.close();
        }

        // might have run the check in the brief period where the indicator was started - clean these up first
        verify(electionStrategy, atMost(Integer.MAX_VALUE)).readLease(any());

        startTime += randomIntBetween(1, 100);
        nextTick.set(startTime);
        indicator.runCheck();
        verify(electionStrategy, never()).readLease(any());

        indicator.close();
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.humanReadable(true);
        xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        XContentParser parser = XContentType.JSON.xContent()
            .createParser(XContentParserConfiguration.EMPTY, BytesReference.bytes(builder).streamInput());
        return parser.map();
    }

    private Long getDuration(Map<String, Object> details, String label) {
        Object value = details.get(label);
        if (value == null) {
            return null;
        }
        if (details.get(label) instanceof Integer integerValue) {
            return integerValue.longValue();
        }
        if (details.get(label) instanceof Long longValue) {
            return longValue;
        }
        fail("Type of " + label + " was not an integer or long as expected, it was " + value.getClass());
        return null;
    }
}
