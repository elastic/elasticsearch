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

package org.elasticsearch.common.joda;

import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
public class DateMathParserTests extends ElasticsearchTestCase {

    @Test
    public void dataMathTests() {
        DateMathParser parser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);

        assertThat(parser.parse("now", 0), equalTo(0l));
        assertThat(parser.parse("now+m", 0), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parse("now+1m", 0), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parse("now+11m", 0), equalTo(TimeUnit.MINUTES.toMillis(11)));

        assertThat(parser.parse("now+1d", 0), equalTo(TimeUnit.DAYS.toMillis(1)));

        assertThat(parser.parse("now+1m+1s", 0), equalTo(TimeUnit.MINUTES.toMillis(1) + TimeUnit.SECONDS.toMillis(1)));
        assertThat(parser.parse("now+1m-1s", 0), equalTo(TimeUnit.MINUTES.toMillis(1) - TimeUnit.SECONDS.toMillis(1)));

        assertThat(parser.parse("now+1m+1s/m", 0), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parseRoundCeil("now+1m+1s/m", 0), equalTo(TimeUnit.MINUTES.toMillis(2)));
        
        assertThat(parser.parse("now+4y", 0), equalTo(TimeUnit.DAYS.toMillis(4*365 + 1)));
    }

    @Test
    public void actualDateTests() {
        DateMathParser parser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);

        assertThat(parser.parse("1970-01-01", 0), equalTo(0l));
        assertThat(parser.parse("1970-01-01||+1m", 0), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parse("1970-01-01||+1m+1s", 0), equalTo(TimeUnit.MINUTES.toMillis(1) + TimeUnit.SECONDS.toMillis(1)));
        
        assertThat(parser.parse("2013-01-01||+1y", 0), equalTo(parser.parse("2013-01-01", 0) + TimeUnit.DAYS.toMillis(365)));
        assertThat(parser.parse("2013-03-03||/y", 0), equalTo(parser.parse("2013-01-01", 0)));
        assertThat(parser.parseRoundCeil("2013-03-03||/y", 0), equalTo(parser.parse("2014-01-01", 0)));
    }
}
