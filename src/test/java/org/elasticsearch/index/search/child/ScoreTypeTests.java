/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.index.search.child;

import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests {@link ScoreType} to ensure backward compatibility of any changes.
 */
public class ScoreTypeTests extends ElasticsearchTestCase {
    @Test
    public void minFromString() {
        assertThat("fromString(min) != MIN", ScoreType.MIN, equalTo(ScoreType.fromString("min")));
    }

    @Test
    public void maxFromString() {
        assertThat("fromString(max) != MAX", ScoreType.MAX, equalTo(ScoreType.fromString("max")));
    }

    @Test
    public void avgFromString() {
        assertThat("fromString(avg) != AVG", ScoreType.AVG, equalTo(ScoreType.fromString("avg")));
    }

    @Test
    public void sumFromString() {
        assertThat("fromString(sum) != SUM", ScoreType.SUM, equalTo(ScoreType.fromString("sum")));
        // allowed for consistency with ScoreMode.Total:
        assertThat("fromString(total) != SUM", ScoreType.SUM, equalTo(ScoreType.fromString("total")));
    }

    @Test
    public void noneFromString() {
        assertThat("fromString(none) != NONE", ScoreType.NONE, equalTo(ScoreType.fromString("none")));
    }

    /**
     * Should throw {@link ElasticsearchIllegalArgumentException} instead of NPE.
     */
    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void nullFromString_throwsException() {
        ScoreType.fromString(null);
    }

    /**
     * Failure should not change (and the value should never match anything...).
     */
    @Test(expected = ElasticsearchIllegalArgumentException.class)
    public void unrecognizedFromString_throwsException() {
        ScoreType.fromString("unrecognized value");
    }
}
