/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAliasAction.Request;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class PutTrainedModelAliasActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    private String modelAlias;

    @Before
    public void setupModelAlias() {
        modelAlias = randomAlphaOfLength(10);
    }

    @Override
    protected Request createTestInstance() {
        return new Request(modelAlias, randomAlphaOfLength(10), randomBoolean());
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    public void testCtor() {
        expectThrows(Exception.class, () -> new Request(null, randomAlphaOfLength(10), randomBoolean()));
        expectThrows(Exception.class, () -> new Request(randomAlphaOfLength(10), null, randomBoolean()));
    }

    public void testValidate() {

        { // model_alias equal to model Id
            ActionRequestValidationException ex = new Request("foo", "foo", randomBoolean()).validate();
            assertThat(ex, not(nullValue()));
            assertThat(ex.getMessage(), containsString("model_alias [foo] cannot equal model_id [foo]"));
        }
        { // model_alias cannot end in numbers
            modelAlias = randomAlphaOfLength(10) + randomIntBetween(0, Integer.MAX_VALUE);
            ActionRequestValidationException ex = new Request(modelAlias, "foo", randomBoolean()).validate();
            assertThat(ex, not(nullValue()));
            assertThat(
                ex.getMessage(),
                containsString(
                    "can contain lowercase alphanumeric (a-z and 0-9), hyphens or underscores; "
                        + "must start with alphanumeric and cannot end with numbers"
                )
            );
        }
    }

}
