/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.contextengine.aiindex.AiIndex;
import org.elasticsearch.xpack.contextengine.aiindex.AiIndexTestUtils;
import org.elasticsearch.xpack.contextengine.aiindex.TypeValue;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class PutAiIndexActionRequestTests extends AbstractWireSerializingTestCase<PutAiIndexAction.Request> {

    @Override
    protected Writeable.Reader<PutAiIndexAction.Request> instanceReader() {
        return PutAiIndexAction.Request::new;
    }

    @Override
    protected PutAiIndexAction.Request createTestInstance() {
        return new PutAiIndexAction.Request(randomAlphaOfLength(10), AiIndexTestUtils.randomAiIndex(), randomBoolean());
    }

    @Override
    protected PutAiIndexAction.Request mutateInstance(PutAiIndexAction.Request instance) {
        return new PutAiIndexAction.Request(
            randomValueOtherThan(instance.id(), () -> randomAlphaOfLength(10)),
            instance.aiIndex(),
            instance.create()
        );
    }

    public void testValidateValidRequest() {
        String id = randomAlphaOfLength(10);
        assertThat(new PutAiIndexAction.Request(id, AiIndexTestUtils.validAiIndex(id), randomBoolean()).validate(), nullValue());
    }

    public void testValidateMissingFields() {
        AiIndex aiIndex = new AiIndex(null, "", null, false, null, null, null, null, null, null, null, null);
        ActionRequestValidationException e = new PutAiIndexAction.Request("", aiIndex, false).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[id] cannot be null or empty"));
        assertThat(e.getMessage(), containsString("[space] cannot be null or empty"));
        assertThat(e.getMessage(), containsString("[dest] is required"));
    }

    public void testValidateIdMismatchAndColons() {
        AiIndex aiIndex = new AiIndex("other", "a:b", null, false, null, null, null, AiIndexTestUtils.randomDest(), null, null, null, null);
        ActionRequestValidationException e = new PutAiIndexAction.Request("my:id", aiIndex, false).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[id] cannot contain [:]"));
        assertThat(e.getMessage(), containsString("[id] in the body must match the id in the URL"));
        assertThat(e.getMessage(), containsString("[space] cannot contain [:]"));
    }

    public void testValidateDestAndTimestamps() {
        AiIndex aiIndex = new AiIndex(
            null,
            "default",
            null,
            false,
            null,
            AiIndexTestUtils.randomTimestamp(),
            null,
            new TypeValue("alias", ""),
            List.of(new TypeValue("workflow", "")),
            null,
            null,
            null
        );
        ActionRequestValidationException e = new PutAiIndexAction.Request("my-index", aiIndex, false).validate();
        assertThat(e, notNullValue());
        assertThat(e.getMessage(), containsString("[dest.type] must be one of [index, data_stream]"));
        assertThat(e.getMessage(), containsString("[dest.value] cannot be null or empty"));
        assertThat(e.getMessage(), containsString("[automations] items must have a non-empty [type] and [value]"));
        assertThat(e.getMessage(), containsString("[date_created] and [date_modified] are set at write time"));
    }
}
