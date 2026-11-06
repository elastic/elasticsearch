/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.contextengine.aiindex;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class AiIndexTests extends AbstractXContentSerializingTestCase<AiIndex> {

    @Override
    protected AiIndex doParseInstance(XContentParser parser) throws IOException {
        return AiIndex.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<AiIndex> instanceReader() {
        return AiIndex::new;
    }

    @Override
    protected AiIndex createTestInstance() {
        return AiIndexTestUtils.randomAiIndex();
    }

    @Override
    protected AiIndex mutateInstance(AiIndex instance) {
        return instance.withIdentityAndDates(
            randomValueOtherThan(instance.id(), () -> randomAlphaOfLength(10)),
            instance.space(),
            instance.dateCreated(),
            instance.dateModified()
        );
    }

    public void testWithIdentityAndDates() {
        AiIndex aiIndex = AiIndexTestUtils.randomAiIndex();
        var created = AiIndexTestUtils.randomTimestamp();
        var modified = AiIndexTestUtils.randomTimestamp();
        AiIndex stored = aiIndex.withIdentityAndDates("the-id", "the-space", created, modified);
        assertEquals("the-id", stored.id());
        assertEquals("the-space", stored.space());
        assertEquals(created, stored.dateCreated());
        assertEquals(modified, stored.dateModified());
        assertEquals(aiIndex.dest(), stored.dest());
        assertEquals(aiIndex.sources(), stored.sources());
    }
}
