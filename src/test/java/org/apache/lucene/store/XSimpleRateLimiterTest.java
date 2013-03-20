/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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
package org.apache.lucene.store;

import java.util.concurrent.TimeUnit;

import org.testng.annotations.Test;
import org.apache.lucene.store.RateLimiter.SimpleRateLimiter;
import org.apache.lucene.util.Version;
import org.elasticsearch.common.lucene.Lucene;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.greaterThan;

public class XSimpleRateLimiterTest {

    @Test
    public void testPause() {
        XSimpleRateLimiter limiter = new XSimpleRateLimiter(10); // 10 MB / Sec
        limiter.pause(2);//init
        long pause = 0;
        for (int i = 0; i < 3; i++) {
            pause += limiter.pause(4 * 1024 * 1024); // fire up 3 * 4 MB 
        }
        final long convert = TimeUnit.MILLISECONDS.convert(pause, TimeUnit.NANOSECONDS);
        assertThat(convert, lessThan(2000l)); // more than 2 seconds should be an error here!
        assertThat(convert, greaterThan(1000l)); // we should sleep at lease 1 sec
    }
    
    @Test
    public void testPauseLucene() {
        if (Version.LUCENE_42 != Lucene.VERSION) { // once we upgrade test the lucene impl again
            SimpleRateLimiter limiter = new SimpleRateLimiter(10); // 10 MB / Sec
            limiter.pause(2);//init
            long pause = 0;
            for (int i = 0; i < 3; i++) {
                pause += limiter.pause(4 * 1024 * 1024); // fire up 3 * 4 MB 
            }
            final long convert = TimeUnit.MILLISECONDS.convert(pause, TimeUnit.NANOSECONDS);
            assertThat(convert, lessThan(2000l)); // more than 2 seconds should be an error here!
            assertThat(convert, greaterThan(1000l)); // we should sleep at lease 1 sec
            assert false : "Upgrade XSimpleRateLimiter to Lucene SimpleRateLimiter";
        }
    }
}
