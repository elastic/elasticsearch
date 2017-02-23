/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.test.rest;

import org.apache.http.HttpStatus;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.elasticsearch.xpack.ml.integration.MlRestTestStateCleaner;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

/** Runs rest tests against external cluster */
public class XPackRestIT extends XPackRestTestCase {

    @After
    public void clearMlState() throws IOException {
        new MlRestTestStateCleaner(logger, client(), this).clearMlMetadata();
    }

    public XPackRestIT(ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

    /**
     * Waits for the Security template to be created by the {@link SecurityLifecycleService}.
     */
    @Before
    public void waitForSecurityTemplate() throws Exception {
        String templateApi = "indices.exists_template";
        Map<String, String> params = singletonMap("name", SecurityLifecycleService.SECURITY_TEMPLATE_NAME);

        AtomicReference<IOException> exceptionHolder = new AtomicReference<>();
        awaitBusy(() -> {
            try {
                ClientYamlTestResponse response = getAdminExecutionContext().callApi(templateApi, params, emptyList(), emptyMap());
                // We don't check the version of the template - it is the right one when testing documentation.
                if (response.getStatusCode() == HttpStatus.SC_OK) {
                    exceptionHolder.set(null);
                    return true;
                }
            } catch (IOException e) {
                exceptionHolder.set(e);
            }
            return false;
        });

        IOException exception = exceptionHolder.get();
        if (exception != null) {
            throw new IllegalStateException("Exception when waiting for security template to be created", exception);
        }
    }
}
