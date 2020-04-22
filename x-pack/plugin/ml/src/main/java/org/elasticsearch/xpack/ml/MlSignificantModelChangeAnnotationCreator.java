/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationPersister;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.ml.process.logging.CppLogMessage;

import java.time.Clock;
import java.time.Instant;
import java.util.Date;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * {@link MlSignificantModelChangeAnnotationCreator} class turns cpp log message related to a significant model change into annotation.
 */
class MlSignificantModelChangeAnnotationCreator {

    /**
     * Pattern which all the log messages related to significant model changes must match.
     */
    private static final Pattern messagePattern =
        Pattern.compile("^.*\\[model_change_annotation\\]\\[([0-9]{1,18})\\]\\[([^/]*)/([^/]*)/([^/]*)\\]\\s(.*)$");

    private final Clock clock;
    private final AnnotationPersister annotationPersister;

    MlSignificantModelChangeAnnotationCreator(Clock clock, AnnotationPersister annotationPersister) {
        this.clock = Objects.requireNonNull(clock);
        this.annotationPersister = Objects.requireNonNull(annotationPersister);
    }

    public void maybeCreateAnnotation(String jobId, CppLogMessage msg) {
        Matcher messageMatcher = messagePattern.matcher(msg.getMessage());
        if (messageMatcher.matches() == false) {
            // This cpp log message is *not* related to a significant model change, do nothing.
            return;
        }
        Annotation annotation = createSignificantModelChangeAnnotation(jobId, messageMatcher);
        annotationPersister.persistAnnotation(
            null,
            annotation,
            "[" + jobId + "] failed to create annotation for significant model change.");
    }

    private Annotation createSignificantModelChangeAnnotation(String jobId, Matcher messageMatcher) {
        assert messageMatcher.matches();
        // Long.parseLong throws NumberFormatException, but it's impossible here as group(1) was matched as [0-9]{1,18} which parses as long
        Date modelChangeTime = Date.from(Instant.ofEpochMilli(Long.parseLong(messageMatcher.group(1))));
        String partition = messageMatcher.group(2);
        String person = messageMatcher.group(3);
        String attribute = messageMatcher.group(4);
        String message = messageMatcher.group(5);
        Date currentTime = new Date(clock.millis());
        return new Annotation.Builder()
            .setAnnotation(
                new ParameterizedMessage("[partition={};person={};attribute={}] {}", partition, person, attribute, message)
                    .getFormattedMessage())
            .setCreateTime(currentTime)
            .setCreateUsername(XPackUser.NAME)
            .setTimestamp(modelChangeTime)
            .setEndTimestamp(modelChangeTime)
            .setJobId(jobId)
            .setModifiedTime(currentTime)
            .setModifiedUsername(XPackUser.NAME)
            .setType("annotation")
            .setLabel(new ParameterizedMessage("{}/{}", partition, person).getFormattedMessage())
            .build();
    }
}
