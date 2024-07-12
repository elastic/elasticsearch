/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.elasticsearch.common.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * Stable public APIs that retain source and binary compatibility within a major release.
 * These interfaces can change from one major release to another major release
 * (e.g. from 1.0 to 2.0). The types marked with this annotations could only expose
 * other {@link PublicApi} or {@link ExperimentalApi} types as public members.
 *
 * @opensearch.api
 */
@Documented
@Target({
    ElementType.TYPE,
    ElementType.PACKAGE,
    ElementType.METHOD,
    ElementType.CONSTRUCTOR,
    ElementType.PARAMETER,
    ElementType.FIELD,
    ElementType.ANNOTATION_TYPE,
    ElementType.MODULE })
@PublicApi(since = "2.10.0")
public @interface PublicApi {
    /**
     * Version when this API was released
     */
    String since();
}
