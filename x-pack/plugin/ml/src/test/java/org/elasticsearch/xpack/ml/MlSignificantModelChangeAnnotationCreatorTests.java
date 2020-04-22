/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.ml.process.logging.CppLogMessage;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Date;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class MlSignificantModelChangeAnnotationCreatorTests extends ESTestCase {

    private static final String JOB_ID = "job_id";
    private static final Instant MSG_TIME = Instant.ofEpochMilli(1000_000_000);
    private static final Instant NOW = Instant.ofEpochMilli(1000_000_002);

    private Clock clock;
    private AnnotationPersister annotationPersister;

    @Before
    public void setUpMocks() {
        clock = Clock.fixed(NOW, ZoneId.systemDefault());
        annotationPersister = mock(AnnotationPersister.class);
    }

    @After
    public void verifyNoMoreInteractionsWithMocks() {
        verifyNoMoreInteractions(annotationPersister);
    }

    public void testMaybeCreateAnnotation_AnnotationCreated() {
        MlSignificantModelChangeAnnotationCreator annotationCreator = createAnnotationCreator();
        CppLogMessage msg =
            createCppLogMessage(
                "text before bracket doesn't matter [model_change_annotation][1000000001][a/b/c] Model change detected");
        annotationCreator.maybeCreateAnnotation(JOB_ID, msg);

        verify(annotationPersister).persistAnnotation(
            null,
            new Annotation.Builder()
                .setAnnotation("[partition=a;person=b;attribute=c] Model change detected")
                .setCreateTime(Date.from(NOW))
                .setCreateUsername(XPackUser.NAME)
                .setTimestamp(Date.from(Instant.ofEpochMilli(1000_000_001)))
                .setEndTimestamp(Date.from(Instant.ofEpochMilli(1000_000_001)))
                .setJobId(JOB_ID)
                .setModifiedTime(Date.from(NOW))
                .setModifiedUsername(XPackUser.NAME)
                .setType("annotation")
                .setLabel("a/b")
                .build(),
            "[" + JOB_ID + "] failed to create annotation for significant model change.");
    }

    public void testMaybeCreateAnnotation_AnnotationNotCreated_NoTimestampNoLabel() {
        MlSignificantModelChangeAnnotationCreator annotationCreator = createAnnotationCreator();
        CppLogMessage msg = createCppLogMessage("[model_change_annotation] Model change detected");
        annotationCreator.maybeCreateAnnotation(JOB_ID, msg);
    }

    public void testMaybeCreateAnnotation_AnnotationNotCreated_BadTimestamp_NotANumber() {
        MlSignificantModelChangeAnnotationCreator annotationCreator = createAnnotationCreator();
        CppLogMessage msg = createCppLogMessage("[model_change_annotation][not-a-number][some-label] Model change detected");
        annotationCreator.maybeCreateAnnotation(JOB_ID, msg);
    }

    public void testMaybeCreateAnnotation_AnnotationNotCreated_BadTimestamp_TooBig() {
        MlSignificantModelChangeAnnotationCreator annotationCreator = createAnnotationCreator();
        CppLogMessage msg = createCppLogMessage("[model_change_annotation][1000000000000000000][some-label] Model change detected");
        annotationCreator.maybeCreateAnnotation(JOB_ID, msg);
    }

    public void testMaybeCreateAnnotation_AnnotationNotCreated_MessageNotRelatedToModelChange() {
        MlSignificantModelChangeAnnotationCreator annotationCreator = createAnnotationCreator();
        CppLogMessage msg = createCppLogMessage("Pruning all models");
        annotationCreator.maybeCreateAnnotation(JOB_ID, msg);
    }

    private MlSignificantModelChangeAnnotationCreator createAnnotationCreator() {
        return new MlSignificantModelChangeAnnotationCreator(clock, annotationPersister);
    }

    private static CppLogMessage createCppLogMessage(String message) {
        CppLogMessage msg = new CppLogMessage(MSG_TIME);
        msg.setMessage(message);
        return msg;
    }
}
