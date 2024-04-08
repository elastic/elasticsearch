/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.job.config;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class BlockedTests extends AbstractXContentSerializingTestCase<Blocked> {

    @Override
    protected Blocked doParseInstance(XContentParser parser) throws IOException {
        return Blocked.STRICT_PARSER.apply(parser, null);
    }

    @Override
    protected Writeable.Reader<Blocked> instanceReader() {
        return Blocked::new;
    }

    @Override
    protected Blocked createTestInstance() {
        return createRandom();
    }

    @Override
    protected Blocked mutateInstance(Blocked instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public static Blocked createRandom() {
        Blocked.Reason reason = randomFrom(Blocked.Reason.values());
        TaskId taskId = (reason != Blocked.Reason.NONE && randomBoolean())
            ? new TaskId(randomAlphaOfLength(10) + ":" + randomNonNegativeLong())
            : null;
        return new Blocked(reason, taskId);
    }

    public void testReasonFromString() {
        assertThat(Blocked.Reason.fromString("NonE"), equalTo(Blocked.Reason.NONE));
        assertThat(Blocked.Reason.fromString("dElETe"), equalTo(Blocked.Reason.DELETE));
        assertThat(Blocked.Reason.fromString("ReSEt"), equalTo(Blocked.Reason.RESET));
        assertThat(Blocked.Reason.fromString("reVERt"), equalTo(Blocked.Reason.REVERT));
    }

    public void testReasonToString() {
        List<String> asStrings = Arrays.stream(Blocked.Reason.values()).map(Blocked.Reason::toString).collect(Collectors.toList());
        assertThat(asStrings, contains("none", "delete", "reset", "revert"));
    }
}
