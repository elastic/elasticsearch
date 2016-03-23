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

package org.elasticsearch.threadpool;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

public class ThreadPoolStatsTests extends ESTestCase {
    public void testThreadPoolStatsSort() throws IOException {
        List<ThreadPoolStats.Stats> stats = new ArrayList<>();
        stats.add(new ThreadPoolStats.Stats("z", -1, 0, 0, 0, 0, 0L));
        stats.add(new ThreadPoolStats.Stats("m", 3, 0, 0, 0, 0, 0L));
        stats.add(new ThreadPoolStats.Stats("m", 1, 0, 0, 0, 0, 0L));
        stats.add(new ThreadPoolStats.Stats("d", -1, 0, 0, 0, 0, 0L));
        stats.add(new ThreadPoolStats.Stats("m", 2, 0, 0, 0, 0, 0L));
        stats.add(new ThreadPoolStats.Stats("t", -1, 0, 0, 0, 0, 0L));
        stats.add(new ThreadPoolStats.Stats("a", -1, 0, 0, 0, 0, 0L));

        List<ThreadPoolStats.Stats> copy = new ArrayList<>(stats);
        Collections.sort(copy);

        List<String> names = new ArrayList<>(copy.size());
        for (ThreadPoolStats.Stats stat : copy) {
            names.add(stat.getName());
        }
        assertThat(names, contains("a", "d", "m", "m", "m", "t", "z"));

        List<Integer> threads = new ArrayList<>(copy.size());
        for (ThreadPoolStats.Stats stat : copy) {
            threads.add(stat.getThreads());
        }
        assertThat(threads, contains(-1, -1, 1, 2, 3,-1,-1));
    }

    public void testThreadPoolStatsToXContent() throws IOException {
        try (BytesStreamOutput os = new BytesStreamOutput()) {

            List<ThreadPoolStats.Stats> stats = new ArrayList<>();
            stats.add(new ThreadPoolStats.Stats(ThreadPool.Names.SEARCH, -1, 0, 0, 0, 0, 0L));
            stats.add(new ThreadPoolStats.Stats(ThreadPool.Names.WARMER, -1, 0, 0, 0, 0, 0L));
            stats.add(new ThreadPoolStats.Stats(ThreadPool.Names.GENERIC, -1, 0, 0, 0, 0, 0L));
            stats.add(new ThreadPoolStats.Stats(ThreadPool.Names.FORCE_MERGE, -1, 0, 0, 0, 0, 0L));
            stats.add(new ThreadPoolStats.Stats(ThreadPool.Names.SAME, -1, 0, 0, 0, 0, 0L));


            try (XContentBuilder builder = new XContentBuilder(XContentType.JSON.xContent(), os)) {
                new ThreadPoolStats(stats).toXContent(builder, ToXContent.EMPTY_PARAMS);
            }

            try (XContentParser parser = XContentType.JSON.xContent().createParser(os.bytes())) {
                XContentParser.Token token = parser.currentToken();
                assertNull(token);

                token = parser.nextToken();
                assertThat(token, equalTo(XContentParser.Token.VALUE_STRING));

                token = parser.nextToken();
                assertThat(token, equalTo(XContentParser.Token.START_OBJECT));

                token = parser.nextToken();
                assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));

                List<String> names = new ArrayList<>();
                while (token == XContentParser.Token.FIELD_NAME) {
                    names.add(parser.currentName());

                    token = parser.nextToken();
                    assertThat(token, equalTo(XContentParser.Token.START_OBJECT));

                    parser.skipChildren();
                    token = parser.nextToken();
                }
                assertThat(names, contains(ThreadPool.Names.FORCE_MERGE,
                        ThreadPool.Names.GENERIC,
                        ThreadPool.Names.SAME,
                        ThreadPool.Names.SEARCH,
                        ThreadPool.Names.WARMER));
            }
        }
    }
}
