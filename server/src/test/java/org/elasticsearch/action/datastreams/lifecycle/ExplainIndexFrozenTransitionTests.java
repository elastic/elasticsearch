/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.datastreams.lifecycle;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ExplainIndexFrozenTransitionTests extends AbstractWireSerializingTestCase<ExplainIndexFrozenTransition> {

    public void testToXContent() throws IOException {
        ExplainIndexFrozenTransition frozenTransition = new ExplainIndexFrozenTransition(
            true,
            true,
            ExplainIndexFrozenTransition.Status.RUNNING
        );
        String serialized = Strings.toString(frozenTransition);
        Map<String, Object> resultMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), serialized, false);
        assertThat(resultMap.get("eligible"), equalTo(true));
        assertThat(resultMap.get("marked_for_transition"), equalTo(true));
        assertThat(resultMap.get("status"), equalTo("running"));
        assertThat(resultMap.containsKey("completed"), equalTo(false));
    }

    public void testStatusToString() {
        assertThat(ExplainIndexFrozenTransition.Status.NOT_STARTED.toString(), equalTo("not_started"));
        assertThat(ExplainIndexFrozenTransition.Status.QUEUED.toString(), equalTo("queued"));
        assertThat(ExplainIndexFrozenTransition.Status.RUNNING.toString(), equalTo("running"));
    }

    @Override
    protected Writeable.Reader<ExplainIndexFrozenTransition> instanceReader() {
        return ExplainIndexFrozenTransition::new;
    }

    @Override
    protected ExplainIndexFrozenTransition createTestInstance() {
        return randomExplainIndexFrozenTransition();
    }

    @Override
    protected ExplainIndexFrozenTransition mutateInstance(ExplainIndexFrozenTransition instance) {
        return randomValueOtherThan(instance, ExplainIndexFrozenTransitionTests::randomExplainIndexFrozenTransition);
    }

    private static ExplainIndexFrozenTransition randomExplainIndexFrozenTransition() {
        return new ExplainIndexFrozenTransition(randomBoolean(), randomBoolean(), randomFrom(ExplainIndexFrozenTransition.Status.values()));
    }
}
