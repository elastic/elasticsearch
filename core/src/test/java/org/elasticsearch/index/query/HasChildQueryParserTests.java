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
package org.elasticsearch.index.query;

import org.apache.lucene.search.join.ScoreMode;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class HasChildQueryParserTests extends ESTestCase {

    @Test
    public void minFromString() {
        assertThat("fromString(min) != MIN", ScoreMode.Min, equalTo(HasChildQueryParser.parseScoreMode("min")));
    }

    @Test
    public void maxFromString() {
        assertThat("fromString(max) != MAX", ScoreMode.Max, equalTo(HasChildQueryParser.parseScoreMode("max")));
    }

    @Test
    public void avgFromString() {
        assertThat("fromString(avg) != AVG", ScoreMode.Avg, equalTo(HasChildQueryParser.parseScoreMode("avg")));
    }

    @Test
    public void sumFromString() {
        assertThat("fromString(total) != SUM", ScoreMode.Total, equalTo(HasChildQueryParser.parseScoreMode("total")));
    }

    @Test
    public void noneFromString() {
        assertThat("fromString(none) != NONE", ScoreMode.None, equalTo(HasChildQueryParser.parseScoreMode("none")));
    }

    /**
     * Should throw {@link IllegalArgumentException} instead of NPE.
     */
    @Test(expected = IllegalArgumentException.class)
    public void nullFromString_throwsException() {
        HasChildQueryParser.parseScoreMode(null);
    }

    /**
     * Failure should not change (and the value should never match anything...).
     */
    @Test(expected = IllegalArgumentException.class)
    public void unrecognizedFromString_throwsException() {
        HasChildQueryParser.parseScoreMode("unrecognized value");
    }
}
