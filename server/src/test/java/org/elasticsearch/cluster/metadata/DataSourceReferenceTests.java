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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class DataSourceReferenceTests extends AbstractXContentSerializingTestCase<DataSourceReference> {

    @Override
    protected Writeable.Reader<DataSourceReference> instanceReader() {
        return DataSourceReference::new;
    }

    @Override
    protected DataSourceReference doParseInstance(XContentParser parser) throws IOException {
        return DataSourceReference.fromXContent(parser);
    }

    @Override
    protected DataSourceReference createTestInstance() {
        return new DataSourceReference(randomIdentifier());
    }

    @Override
    protected DataSourceReference mutateInstance(DataSourceReference instance) {
        return new DataSourceReference(randomValueOtherThan(instance.getName(), ESTestCase::randomIdentifier));
    }

    public void testRequiresName() {
        expectThrows(NullPointerException.class, () -> new DataSourceReference((String) null));
    }

    public void testToString() {
        assertEquals("[my-source]", new DataSourceReference("my-source").toString());
    }

    public void testEqualsAndHashCode() {
        DataSourceReference a = new DataSourceReference("x");
        DataSourceReference b = new DataSourceReference("x");
        DataSourceReference c = new DataSourceReference("y");
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
        assertNotEquals(a, c);
    }
}
