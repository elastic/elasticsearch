/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write.engine;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;

public class LinkageFailuresTests extends ESTestCase {

    public void testAMissingClassIsNamed() {
        final NoClassDefFoundError error = new NoClassDefFoundError("org/locationtech/jts/io/ParseException");

        final IllegalStateException exception = LinkageFailures.asException("write a Parquet file", error);

        assertThat(exception.getCause(), sameInstance(error));
        assertThat(exception.getMessage(), containsString("write a Parquet file"));
        assertThat(exception.getMessage(), containsString("the class [org/locationtech/jts/io/ParseException] is not available"));
        assertThat(exception.getMessage(), containsString("esql-datasource-iceberg"));
    }

    public void testEveryOtherLinkageErrorBecomesAnException() {
        final LinkageError[] errors = new LinkageError[] {
            new ExceptionInInitializerError("static initializer failed"),
            new UnsatisfiedLinkError("no snappyjava in java.library.path"),
            new IncompatibleClassChangeError("class A has changed"),
            new NoSuchMethodError("method m is missing"),
            new LinkageError("generic") };
        for (LinkageError error : errors) {
            final IllegalStateException exception = LinkageFailures.asException("merge Parquet files", error);

            assertThat(exception.getCause(), sameInstance(error));
            assertThat(exception.getMessage(), containsString("merge Parquet files"));
            assertThat(exception.getMessage(), containsString(error.getClass().getName()));
            assertThat(exception.getMessage(), containsString("could not be linked or initialized"));
        }
    }
}
