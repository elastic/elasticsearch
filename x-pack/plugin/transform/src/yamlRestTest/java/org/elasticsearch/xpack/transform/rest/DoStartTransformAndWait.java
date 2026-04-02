/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.xcontent.XContentLocation;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.transform.transforms.TransformStats;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Wraps a {@code transform.start_transform} {@link DoSection} so that after the start call succeeds,
 * we poll {@code GET _transform/{id}/_stats} until the transform state is no longer {@code "stopped"}.
 * <p>
 * This is necessary because the start API can take some number of seconds after the persistent task
 * is assigned, but the transform may not have fully initialized yet. YAML REST tests have no built-in
 * retry mechanism, so without this wrapper, assertions on transform state immediately after
 * start can fail intermittently.
 * <p>
 * Polling uses a separate admin {@link RestClient} so as not to overwrite the test execution
 * context's last response (which subsequent YAML assertions read from).
 */
final class DoStartTransformAndWait implements ExecutableSection {

    private static final Logger logger = LogManager.getLogger(DoStartTransformAndWait.class);

    private final DoSection delegate;
    private final Supplier<RestClient> adminClientSupplier;

    DoStartTransformAndWait(DoSection delegate, Supplier<RestClient> adminClientSupplier) {
        assert "transform.start_transform".equals(delegate.getApiCallSection().getApi());
        this.delegate = delegate;
        this.adminClientSupplier = adminClientSupplier;
    }

    @Override
    public XContentLocation getLocation() {
        return delegate.getLocation();
    }

    @Override
    public void execute(ClientYamlTestExecutionContext executionContext) throws IOException {
        delegate.execute(executionContext);

        if (delegate.getCatch() != null) {
            return;
        }

        String transformId = delegate.getApiCallSection().getParams().get("transform_id");
        if (transformId == null) {
            return;
        }

        try {
            ESTestCase.assertBusy(() -> {
                try {
                    Map<String, Object> transformStats = fetchFirstTransformStats(transformId);
                    if (hasBeenInitialized(transformStats)) {
                        return;
                    }
                    throw new AssertionError(
                        "Transform [" + transformId + "] has not initialized yet (state=[stopped], no evidence of execution)"
                    );
                } catch (IOException e) {
                    throw new AssertionError("Failed to get transform stats for [" + transformId + "]", e);
                }
            }, 30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while waiting for transform to initialize", e);
        } catch (AssertionError busyFailure) {
            final Map<String, Object> finalStats;
            try {
                finalStats = fetchFirstTransformStats(transformId);
            } catch (IOException e) {
                AssertionError err = new AssertionError("Failed to fetch transform stats after wait for [" + transformId + "]", e);
                err.addSuppressed(busyFailure);
                throw err;
            }
            String state = (String) finalStats.get("state");
            if ("failed".equals(state)) {
                throw new AssertionError("Transform [" + transformId + "] is in failed state after wait; stats=" + finalStats, busyFailure);
            }
            if (hasBeenInitialized(finalStats)) {
                throw new AssertionError(
                    "Transform [" + transformId + "] appears initialized after timeout; stats=" + finalStats,
                    busyFailure
                );
            }
            logger.warn(
                "Transform [{}] still appears uninitialized after wait (state=[{}], reason=[{}]); assuming fast-completing batch edge case",
                transformId,
                state,
                finalStats.get(TransformStats.REASON_FIELD.getPreferredName())
            );
        } catch (Exception e) {
            throw new IOException("unexpected exception while waiting for transform to initialize", e);
        }
    }

    private Map<String, Object> fetchFirstTransformStats(String transformId) throws IOException {
        Request statsRequest = new Request("GET", "/_transform/" + transformId + "/_stats");
        Response statsResponse = adminClientSupplier.get().performRequest(statsRequest);
        Map<String, Object> body = XContentHelper.convertToMap(XContentType.JSON.xContent(), statsResponse.getEntity().getContent(), false);
        Object transformsObj = body.get("transforms");
        if (transformsObj == null) {
            throw new AssertionError(
                "Expected [transforms] in stats response for transform [" + transformId + "]; top-level keys were " + body.keySet()
            );
        }
        if (transformsObj instanceof List<?> == false) {
            throw new AssertionError(
                "Expected [transforms] to be a list for transform [" + transformId + "], was [" + transformsObj.getClass().getName() + "]"
            );
        }
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> transforms = (List<Map<String, Object>>) transformsObj;
        if (transforms.size() != 1) {
            throw new AssertionError(
                "Expected exactly one transform in stats for [" + transformId + "], but got size [" + transforms.size() + "]"
            );
        }
        return transforms.get(0);
    }

    @SuppressWarnings("unchecked")
    private static boolean hasBeenInitialized(Map<String, Object> transformStats) {
        String state = (String) transformStats.get("state");
        if ("stopped".equals(state) == false) {
            return true;
        }
        Map<String, Object> stats = (Map<String, Object>) transformStats.get("stats");
        if (stats != null) {
            if (numberGreaterThanZero(stats.get("trigger_count")) || numberGreaterThanZero(stats.get("pages_processed"))) {
                return true;
            }
        }
        Map<String, Object> checkpointing = (Map<String, Object>) transformStats.get("checkpointing");
        if (checkpointing != null) {
            Map<String, Object> last = (Map<String, Object>) checkpointing.get("last");
            if (last != null && numberGreaterThanZero(last.get("checkpoint"))) {
                return true;
            }
        }
        return false;
    }

    private static boolean numberGreaterThanZero(Object value) {
        return value instanceof Number n && n.longValue() > 0;
    }
}
