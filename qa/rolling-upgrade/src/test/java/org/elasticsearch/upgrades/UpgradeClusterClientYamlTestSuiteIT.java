/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.http.HttpStatus;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.xpack.ml.MachineLearningTemplateRegistry;
import org.elasticsearch.xpack.security.SecurityClusterClientYamlTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonMap;

@TimeoutSuite(millis = 5 * TimeUnits.MINUTE) // to account for slow as hell VMs
public class UpgradeClusterClientYamlTestSuiteIT extends SecurityClusterClientYamlTestCase {

    /**
     * Waits for the Machine Learning templates to be created by {@link MachineLearningTemplateRegistry}
     */
    @Before
    public void waitForTemplates() throws Exception {
        List<String> templates = new ArrayList<>();
        templates.addAll(Arrays.asList(MachineLearningTemplateRegistry.TEMPLATE_NAMES));

        for (String template : templates) {
            awaitCallApi("indices.exists_template", singletonMap("name", template), emptyList(),
                    response -> true,
                    () -> "Exception when waiting for [" + template + "] template to be created");
        }
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    public UpgradeClusterClientYamlTestSuiteIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws Exception {
        return createParameters();
    }

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString("elastic:changeme".getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                .build();
    }

    /**
     * Executes an API call using the admin context, waiting for it to succeed.
     */
    private void awaitCallApi(String apiName,
                              Map<String, String> params,
                              List<Map<String, Object>> bodies,
                              CheckedFunction<ClientYamlTestResponse, Boolean, IOException> success,
                              Supplier<String> error) throws Exception {

        AtomicReference<IOException> exceptionHolder = new AtomicReference<>();
        awaitBusy(() -> {
            try {
                ClientYamlTestResponse response = getAdminExecutionContext().callApi(apiName, params, bodies, Collections.emptyMap());
                if (response.getStatusCode() == HttpStatus.SC_OK) {
                    exceptionHolder.set(null);
                    return success.apply(response);
                }
                return false;
            } catch (IOException e) {
                exceptionHolder.set(e);
            }
            return false;
        });

        IOException exception = exceptionHolder.get();
        if (exception != null) {
            throw new IllegalStateException(error.get(), exception);
        }
    }
}
