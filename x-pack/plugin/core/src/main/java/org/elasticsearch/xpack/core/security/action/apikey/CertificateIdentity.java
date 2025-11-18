/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.core.Nullable;

import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

public record CertificateIdentity(@Nullable String value) {

    public CertificateIdentity {
        if (value != null) {
            try {
                Pattern.compile(value);
            } catch (PatternSyntaxException e) {
                throw new IllegalArgumentException("Invalid certificate_identity format: [" + value + "]. Must be a valid regex.", e);
            }
        }
    }
}
