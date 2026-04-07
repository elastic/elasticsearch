/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.grouping;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.AbstractExpressionSerializationTests;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.io.IOException;
import java.time.Duration;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class BucketSerializationTests extends AbstractExpressionSerializationTests<Bucket> {
    @Override
    protected Bucket createTestInstance() {
        return createRandomBucket(configuration());
    }

    public static Bucket createRandomBucket(Configuration configuration) {
        Source source = randomSource();
        Expression field = randomChild();
        Expression buckets = randomChild();
        Expression from = randomChild();
        Expression to = randomChild();
        long offset = randomLongBetween(-Duration.ofDays(1).toMillis(), Duration.ofDays(1).toMillis());
        return new Bucket(source, field, buckets, from, to, configuration, offset);
    }

    @Override
    protected Bucket mutateInstance(Bucket instance) throws IOException {
        Source source = instance.source();
        Expression field = instance.field();
        Expression buckets = instance.buckets();
        Expression from = instance.from();
        Expression to = instance.to();
        long offset = instance.offset();
        switch (between(0, 4)) {
            case 0 -> field = randomValueOtherThan(field, AbstractExpressionSerializationTests::randomChild);
            case 1 -> buckets = randomValueOtherThan(buckets, AbstractExpressionSerializationTests::randomChild);
            case 2 -> from = randomValueOtherThan(from, AbstractExpressionSerializationTests::randomChild);
            case 3 -> to = randomValueOtherThan(to, AbstractExpressionSerializationTests::randomChild);
            case 4 -> offset = randomValueOtherThan(
                offset,
                () -> randomLongBetween(-Duration.ofDays(1).toMillis(), Duration.ofDays(1).toMillis())
            );
        }
        return new Bucket(source, field, buckets, from, to, configuration(), offset);
    }

    public void testOffsetBackcompatSerialization() throws IOException {
        Bucket instance = new Bucket(randomSource(), randomChild(), randomChild(), randomChild(), randomChild(), configuration(), 0L);
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(Bucket.ESQL_BUCKET_OFFSET);
        Bucket copy = copyInstance(instance, oldVersion);
        assertThat(copy.offset(), equalTo(0L));
    }

    public void testOffsetBackcompatSerializationRejectsNonZeroOffset() throws IOException {
        Bucket instance = new Bucket(
            randomSource(),
            randomChild(),
            randomChild(),
            randomChild(),
            randomChild(),
            configuration(),
            randomLongBetween(1, 1000)
        );
        TransportVersion oldVersion = TransportVersionUtils.getPreviousVersion(Bucket.ESQL_BUCKET_OFFSET);
        EsqlIllegalArgumentException e = expectThrows(EsqlIllegalArgumentException.class, () -> copyInstance(instance, oldVersion));
        assertThat(e.getMessage(), containsString("bucket with offset is not supported in peer node's version [" + oldVersion + "]"));
    }
}
