/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.job.persistence;

import org.elasticsearch.test.ESTestCase;
import org.junit.Assert;

public class BucketQueryBuilderTests extends ESTestCase {

    public void testDefaultBuild() throws Exception {
        BucketQueryBuilder.BucketQuery query = new BucketQueryBuilder("1000").build();

        Assert.assertEquals("1000", query.getTimestamp());
        assertEquals(false, query.isIncludeInterim());
        assertEquals(false, query.isExpand());
        assertEquals(null, query.getPartitionValue());
    }

    public void testDefaultAll() throws Exception {
        BucketQueryBuilder.BucketQuery query =
                new BucketQueryBuilder("1000")
                .expand(true)
                .includeInterim(true)
                .partitionValue("p")
                .build();

        Assert.assertEquals("1000", query.getTimestamp());
        assertEquals(true, query.isIncludeInterim());
        assertEquals(true, query.isExpand());
        assertEquals("p", query.getPartitionValue());
    }

    public void testEqualsHash() throws Exception {
        BucketQueryBuilder.BucketQuery query =
                new BucketQueryBuilder("1000")
                .expand(true)
                .includeInterim(true)
                .partitionValue("p")
                .build();

        BucketQueryBuilder.BucketQuery query2 =
                new BucketQueryBuilder("1000")
                .expand(true)
                .includeInterim(true)
                .partitionValue("p")
                .build();

        assertEquals(query2, query);
        assertEquals(query2.hashCode(), query.hashCode());

        query2 =
                new BucketQueryBuilder("1000")
                .expand(true)
                .includeInterim(true)
                .partitionValue("q")
                .build();

        assertFalse(query2.equals(query));
        assertFalse(query2.hashCode() == query.hashCode());
    }
}
