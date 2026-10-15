/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.fetch.subphase.FieldAndFormat;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.esql.plan.logical.Sequence;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/** Verifies internal projections retain exact formats across transport and remain unavailable to public search requests. */
public class SequenceDocValuesSearchExtTests extends AbstractWireSerializingTestCase<SequenceDocValuesSearchExt> {
    @Override
    protected SequenceDocValuesSearchExt createTestInstance() {
        List<FieldAndFormat> fields = new ArrayList<>();
        for (int field = 0, count = between(0, 150); field < count; field++) {
            fields.add(new FieldAndFormat("field" + field, randomBoolean() ? null : "strict_date_optional_time_nanos"));
        }
        return new SequenceDocValuesSearchExt(fields, randomFrom(MappedFieldType.FieldExtractPreference.values()));
    }

    @Override
    protected SequenceDocValuesSearchExt mutateInstance(SequenceDocValuesSearchExt instance) {
        List<FieldAndFormat> changed = new ArrayList<>(instance.fields());
        changed.add(new FieldAndFormat("field" + changed.size(), null));
        return new SequenceDocValuesSearchExt(changed, instance.fieldExtractPreference());
    }

    @Override
    protected Writeable.Reader<SequenceDocValuesSearchExt> instanceReader() {
        return SequenceDocValuesSearchExt::new;
    }

    public void testRejectUnsupportedPeerBeforeWriting() throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.getPreviousVersion(Sequence.ESQL_SEQUENCE));
            assertThat(
                expectThrows(IllegalArgumentException.class, () -> createTestInstance().writeTo(out)).getMessage(),
                containsString("SEQUENCE is not supported")
            );
            assertEquals(0, out.size());
        }
    }

    public void testPublicSearchCannotConstructProjection() throws Exception {
        try (var parser = createParser(JsonXContent.jsonXContent, "{}")) {
            parser.nextToken();
            assertThat(
                expectThrows(XContentParseException.class, () -> SequenceDocValuesSearchExt.fromXContent(parser)).getMessage(),
                containsString("reserved for internal ES|QL searches")
            );
        }
    }

    public void testExtractionPreferenceParticipatesInProjectionIdentity() {
        var fields = List.of(new FieldAndFormat("field", null));
        assertNotEquals(
            new SequenceDocValuesSearchExt(fields, MappedFieldType.FieldExtractPreference.NONE),
            new SequenceDocValuesSearchExt(fields, MappedFieldType.FieldExtractPreference.STORED)
        );
    }
}
