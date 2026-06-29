/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;

public class DatasetFieldMappingTests extends AbstractXContentSerializingTestCase<DatasetFieldMapping> {

    @Override
    protected DatasetFieldMapping doParseInstance(XContentParser parser) throws IOException {
        return DatasetFieldMapping.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<DatasetFieldMapping> instanceReader() {
        return DatasetFieldMapping::new;
    }

    @Override
    protected DatasetFieldMapping createTestInstance() {
        return new DatasetFieldMapping(
            randomFrom("keyword", "long", "integer", "double", "boolean", "date"),
            randomBoolean() ? null : randomAlphaOfLength(6).toLowerCase(Locale.ROOT)
        );
    }

    @Override
    protected DatasetFieldMapping mutateInstance(DatasetFieldMapping instance) {
        if (randomBoolean()) {
            return new DatasetFieldMapping(
                randomValueOtherThan(instance.type(), () -> randomFrom("keyword", "long", "integer", "double", "boolean", "date")),
                instance.source()
            );
        }
        return new DatasetFieldMapping(
            instance.type(),
            randomValueOtherThan(instance.source(), () -> randomBoolean() ? null : randomAlphaOfLength(7).toLowerCase(Locale.ROOT))
        );
    }

    public void testTypeRequired() {
        expectThrows(NullPointerException.class, () -> new DatasetFieldMapping(null, "src"));
    }
}
