/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;
import org.elasticsearch.xpack.core.ml.action.PutJobAction.Request;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.security.cloud.CloudCredential;

import java.io.IOException;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.core.ml.job.config.JobTests.randomValidJobId;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutJobActionRequestTests extends AbstractBWCWireSerializationTestCase<Request> {

    private final String jobId = randomValidJobId();

    @Override
    protected Request createTestInstance() {
        Job.Builder jobConfiguration = buildJobBuilder(jobId, null);
        return new Request(jobConfiguration);
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstanceForVersion(Request instance, TransportVersion version) {
        // cloudCredential is excluded from equals(); its wire behaviour is asserted explicitly below.
        return instance;
    }

    public void testCloudCredentialShouldSurviveTransportRoundTrip() throws IOException {
        Request request = createTestInstance();
        request.setCloudCredential(new CloudCredential(new SecureString("caller-uiam-token".toCharArray())));

        Request deserialized = copyWriteable(request, getNamedWriteableRegistry(), instanceReader(), Request.ML_PUT_JOB_CLOUD_CREDENTIAL);

        assertThat(deserialized.getCloudCredential(), notNullValue());
        assertThat(deserialized.getCloudCredential().value().toString(), equalTo("caller-uiam-token"));
    }

    public void testAbsentCloudCredentialShouldStayAbsentAfterRoundTrip() throws IOException {
        Request request = createTestInstance();

        Request deserialized = copyWriteable(
            request,
            getNamedWriteableRegistry(),
            instanceReader(),
            TransportVersionUtils.randomVersionSupporting(Request.ML_PUT_JOB_CLOUD_CREDENTIAL)
        );

        assertThat(deserialized.getCloudCredential(), nullValue());
    }

    public void testCloudCredentialOnOlderTransportVersionShouldNotBeWritten() throws IOException {
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(Request.ML_PUT_JOB_CLOUD_CREDENTIAL);
        Request request = createTestInstance();
        request.setCloudCredential(new CloudCredential(new SecureString("caller-uiam-token".toCharArray())));

        Request deserialized = copyWriteable(request, getNamedWriteableRegistry(), instanceReader(), oldVersion);

        assertThat(deserialized.getCloudCredential(), nullValue());
        assertThat(deserialized, equalTo(request));
    }

    public void testCloseShouldZeroCloudCredential() {
        Request request = createTestInstance();
        SecureString secret = new SecureString("caller-uiam-token".toCharArray());
        request.setCloudCredential(new CloudCredential(secret));

        request.close();

        expectThrows(IllegalStateException.class, secret::length);
    }

}
