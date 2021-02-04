/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelAliasAction.Request;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class UpdateTrainedModelAliasActionRequestTests extends AbstractSerializingTestCase<Request> {

    private String modelAlias;

    @Before
    public void setupModelAlias() {
        modelAlias = randomAlphaOfLength(10);
    }

    @Override
    protected Request createTestInstance() {
        return new Request(
            modelAlias,
            randomAlphaOfLength(10),
            randomBoolean() ? null : randomAlphaOfLength(10)
        );
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request doParseInstance(XContentParser parser) {
        return Request.fromXContent(modelAlias, parser);
    }

    public void testCtor() {
        expectThrows(Exception.class, () -> new Request(null, randomAlphaOfLength(10), randomBoolean() ? null : randomAlphaOfLength(10)));
        expectThrows(Exception.class, () -> new Request(randomAlphaOfLength(10), null, randomBoolean() ? null : randomAlphaOfLength(10)));
    }

    public void testValidate() {

        {// model ids equal
            ActionRequestValidationException ex = new Request("foo", "bar", "bar").validate();
            assertThat(ex, not(nullValue()));
            assertThat(ex.getMessage(), containsString("old_model_id [bar] cannot equal new_model_id [bar]"));
        }
        { // model_alias equal to either model Id
            ActionRequestValidationException ex = new Request("foo", "foo", "bar").validate();
            assertThat(ex, not(nullValue()));
            assertThat(ex.getMessage(), containsString("model_alias [foo] cannot equal new_model_id [foo] nor old_model_id [bar]"));

            ex = new Request("foo", "bar", "foo").validate();
            assertThat(ex, not(nullValue()));
            assertThat(ex.getMessage(), containsString("model_alias [foo] cannot equal new_model_id [bar] nor old_model_id [foo]"));
        }
        { // model_alias cannot end in numbers
            String modelAlias = randomAlphaOfLength(10) + randomIntBetween(0, Integer.MAX_VALUE);
            ActionRequestValidationException ex = new Request(modelAlias, "foo", "bar").validate();
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
