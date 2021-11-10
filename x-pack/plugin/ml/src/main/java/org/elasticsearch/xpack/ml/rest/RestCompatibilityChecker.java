/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.rest;

import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.rest.RestRequest;

import java.util.function.BiFunction;
import java.util.function.Consumer;

public final class RestCompatibilityChecker {

    private RestCompatibilityChecker() {}

    public static <T> void checkAndSetDeprecatedParam(
        String deprecatedParam,
        String newParam,
        RestApiVersion compatVersion,
        RestRequest restRequest,
        BiFunction<RestRequest, String, T> extractor,
        Consumer<T> setter
    ) {
        final T paramValue;
        if (restRequest.getRestApiVersion() == compatVersion && restRequest.hasParam(deprecatedParam)) {
            LoggingDeprecationHandler.INSTANCE.logRenamedField(null, () -> null, deprecatedParam, newParam, true);
            paramValue = extractor.apply(restRequest, deprecatedParam);
        } else {
            paramValue = extractor.apply(restRequest, newParam);
        }
        setter.accept(paramValue);
    }

}
