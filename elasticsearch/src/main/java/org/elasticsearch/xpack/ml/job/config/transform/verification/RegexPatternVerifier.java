/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.config.transform.verification;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.xpack.ml.job.messages.Messages;
import org.elasticsearch.xpack.ml.job.config.transform.TransformConfig;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public class RegexPatternVerifier implements ArgumentVerifier {
    @Override
    public void verify(String arg, TransformConfig tc) throws ElasticsearchParseException {
        try {
            Pattern.compile(arg);
        } catch (PatternSyntaxException e) {
            String msg = Messages.getMessage(Messages.JOB_CONFIG_TRANSFORM_INVALID_ARGUMENT, tc.getTransform(), arg);
            throw new IllegalArgumentException(msg);
        }
    }
}
