/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.job.process.autodetect.writer;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.job.config.MlFilter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class MlFilterWriterTests extends ESTestCase {

    public void testWrite_GivenEmpty() throws IOException {
        StringBuilder buffer = new StringBuilder();
        new MlFilterWriter(Collections.emptyList(), buffer).write();

        assertThat(buffer.toString().isEmpty(), is(true));
    }

    public void testWrite() throws IOException {
        List<MlFilter> filters = new ArrayList<>();
        filters.add(new MlFilter("filter_1", Arrays.asList("a", "b")));
        filters.add(new MlFilter("filter_2", Arrays.asList("c", "d")));

        StringBuilder buffer = new StringBuilder();
        new MlFilterWriter(filters, buffer).write();

        assertThat(buffer.toString(), equalTo("filter.filter_1 = [\"a\",\"b\"]\nfilter.filter_2 = [\"c\",\"d\"]\n"));
    }
}