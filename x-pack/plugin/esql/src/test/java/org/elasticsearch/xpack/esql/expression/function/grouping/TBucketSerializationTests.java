/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.MetadataAttribute;
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.core.expression.Nullability;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DateEsField;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;

import java.io.IOException;
import java.util.Map;

public class TBucketSerializationTests extends AbstractExpressionSerializationTests<TBucket> {
    @Override
    protected TBucket createTestInstance() {
        return createRandomTBucket();
    }

    public static TBucket createRandomTBucket() {
        Source source = randomSource();
        Expression field = createTimestampFieldAttribute();
        Expression buckets = randomChild();

        return new TBucket(source, field, buckets);
    }

    @Override
    protected TBucket mutateInstance(TBucket instance) throws IOException {
        Source source = instance.source();
        Expression field = createTimestampFieldAttribute();
        Expression buckets = instance.buckets();
        buckets = randomValueOtherThan(buckets, AbstractExpressionSerializationTests::randomChild);

        return new TBucket(source, field, buckets);
    }

    private static FieldAttribute createTimestampFieldAttribute() {
        return new FieldAttribute(
            randomSource(),
            randomAlphaOfLength(10),
            MetadataAttribute.TIMESTAMP_FIELD,
            DateEsField.dateEsField(MetadataAttribute.TIMESTAMP_FIELD, Map.of(), true),
            Nullability.TRUE,
            new NameId(),
            false
        );
    }
}
