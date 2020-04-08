/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.wildcard.mapper;

import java.util.Locale;

/**
 * Thrown when the filter is unable to accelerate a regex and
 * rejectUnaccelerated is set.
 */
public class UnableToAccelerateRegexException extends RuntimeException {
    public UnableToAccelerateRegexException(String regex, int gramSize, String ngramField) {
        super(String.format(Locale.ROOT, "Unable to accelerate \"%s\" with %s sized grams stored in %s", regex, gramSize, ngramField));
    }
}
