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

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class DateMathParserTests extends ElasticsearchTestCase {

    private static Callable<Long> callable(final long value) {
        return new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                return value;
            }
        };
    }

    @Test
    public void dataMathTests() {
        DateMathParser parser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);

        assertThat(parser.parse("now", callable(0)), equalTo(0l));
        assertThat(parser.parse("now+m", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parse("now+1m", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parse("now+11m", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(11)));

        assertThat(parser.parse("now+1d", callable(0)), equalTo(TimeUnit.DAYS.toMillis(1)));

        assertThat(parser.parse("now+1m+1s", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1) + TimeUnit.SECONDS.toMillis(1)));
        assertThat(parser.parse("now+1m-1s", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1) - TimeUnit.SECONDS.toMillis(1)));

        assertThat(parser.parse("now+1m+1s/m", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parseRoundCeil("now+1m+1s/m", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(2)));
        
        assertThat(parser.parse("now+4y", callable(0)), equalTo(TimeUnit.DAYS.toMillis(4*365 + 1)));
    }

    @Test
    public void actualDateTests() {
        DateMathParser parser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);

        assertThat(parser.parse("1970-01-01", callable(0)), equalTo(0l));
        assertThat(parser.parse("1970-01-01||+1m", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parse("1970-01-01||+1m+1s", callable(0)), equalTo(TimeUnit.MINUTES.toMillis(1) + TimeUnit.SECONDS.toMillis(1)));
        
        assertThat(parser.parse("2013-01-01||+1y", callable(0)), equalTo(parser.parse("2013-01-01", callable(0)) + TimeUnit.DAYS.toMillis(365)));
        assertThat(parser.parse("2013-03-03||/y", callable(0)), equalTo(parser.parse("2013-01-01", callable(0))));
        assertThat(parser.parseRoundCeil("2013-03-03||/y", callable(0)), equalTo(parser.parse("2014-01-01", callable(0))));
    }

    public void testOnlyCallsNowIfNecessary() {
        final AtomicBoolean called = new AtomicBoolean();
        final Callable<Long> now = new Callable<Long>() {
            @Override
            public Long call() throws Exception {
                called.set(true);
                return 42L;
            }
        };
        DateMathParser parser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);
        parser.parse("2014-11-18T14:27:32", now);
        assertFalse(called.get());
        parser.parse("now/d", now);
        assertTrue(called.get());
    }
}
