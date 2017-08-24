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

package org.elasticsearch.example.rescore;

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.function.Supplier;

public class ExampleRescoreBuilderTests extends AbstractWireSerializingTestCase<ExampleRescoreBuilder> {
    @Override
    protected ExampleRescoreBuilder createTestInstance() {
        return new ExampleRescoreBuilder(randomFloat()).windowSize(between(0, Integer.MAX_VALUE));
    }

    @Override
    protected Reader<ExampleRescoreBuilder> instanceReader() {
        return ExampleRescoreBuilder::new;
    }

    @Override
    protected ExampleRescoreBuilder mutateInstance(ExampleRescoreBuilder instance) throws IOException {
        @SuppressWarnings("unchecked")
        Supplier<ExampleRescoreBuilder> supplier = randomFrom(
                () -> new ExampleRescoreBuilder(instance.factor())
                        .windowSize(randomValueOtherThan(instance.windowSize(), () -> between(0, Integer.MAX_VALUE))),
                () -> new ExampleRescoreBuilder(randomValueOtherThan(instance.factor(), ESTestCase::randomFloat)));
        return supplier.get();
    }

    public void testRewrite() throws IOException {
        ExampleRescoreBuilder builder = createTestInstance();
        assertSame(builder, builder.rewrite(null));
    }

    public void testRescore() throws IOException {
        ExampleRescoreBuilder builder = new ExampleRescoreBuilder(randomFloat()).windowSize(2);
        RescoreContext context = builder.buildContext(null);
        TopDocs docs = new TopDocs(10, new ScoreDoc[3], 0);
        docs.scoreDocs[0] = new ScoreDoc(0, 1.0f);
        docs.scoreDocs[1] = new ScoreDoc(1, 1.0f);
        docs.scoreDocs[2] = new ScoreDoc(2, 1.0f);
        context.rescorer().rescore(docs, null, context);
        assertEquals(builder.factor(), docs.scoreDocs[0].score, 0.0f);
        assertEquals(builder.factor(), docs.scoreDocs[1].score, 0.0f);
        assertEquals(1.0f, docs.scoreDocs[2].score, 0.0f);
    }
}
