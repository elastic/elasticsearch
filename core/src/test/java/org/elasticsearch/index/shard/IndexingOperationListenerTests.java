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
package org.elasticsearch.index.shard;

import org.apache.lucene.index.Term;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class IndexingOperationListenerTests extends ESTestCase{

    // this test also tests if calls are correct if one or more listeners throw exceptions
    public void testListenersAreExecuted() {
        AtomicInteger preIndex = new AtomicInteger();
        AtomicInteger postIndex = new AtomicInteger();
        AtomicInteger postIndexException = new AtomicInteger();
        AtomicInteger preDelete = new AtomicInteger();
        AtomicInteger postDelete = new AtomicInteger();
        AtomicInteger postDeleteException = new AtomicInteger();
        IndexingOperationListener listener = new IndexingOperationListener() {
            @Override
            public Engine.Index preIndex(Engine.Index operation) {
                preIndex.incrementAndGet();
                return operation;
            }

            @Override
            public void postIndex(Engine.Index index, boolean created) {
                postIndex.incrementAndGet();
            }

            @Override
            public void postIndex(Engine.Index index, Throwable ex) {
                postIndexException.incrementAndGet();
            }

            @Override
            public Engine.Delete preDelete(Engine.Delete delete) {
                preDelete.incrementAndGet();
                return delete;
            }

            @Override
            public void postDelete(Engine.Delete delete) {
                postDelete.incrementAndGet();
            }

            @Override
            public void postDelete(Engine.Delete delete, Throwable ex) {
                postDeleteException.incrementAndGet();
            }
        };

        IndexingOperationListener throwingListener = new IndexingOperationListener() {
            @Override
            public Engine.Index preIndex(Engine.Index operation) {
                throw new RuntimeException();
            }

            @Override
            public void postIndex(Engine.Index index, boolean created) {
                throw new RuntimeException();            }

            @Override
            public void postIndex(Engine.Index index, Throwable ex) {
                throw new RuntimeException();            }

            @Override
            public Engine.Delete preDelete(Engine.Delete delete) {
                throw new RuntimeException();
            }

            @Override
            public void postDelete(Engine.Delete delete) {
                throw new RuntimeException();            }

            @Override
            public void postDelete(Engine.Delete delete, Throwable ex) {
                throw new RuntimeException();
            }
        };
        final List<IndexingOperationListener> indexingOperationListeners = new ArrayList<>(Arrays.asList(listener, listener));
        if (randomBoolean()) {
            indexingOperationListeners.add(throwingListener);
            if (randomBoolean()) {
                indexingOperationListeners.add(throwingListener);
            }
        }
        Collections.shuffle(indexingOperationListeners, random());
        IndexingOperationListener.CompositeListener compositeListener = new IndexingOperationListener.CompositeListener(indexingOperationListeners, logger);
        Engine.Delete delete = new Engine.Delete("test", "1", new Term("_uid", "1"));
        Engine.Index index = new Engine.Index(new Term("_uid", "1"), null);
        compositeListener.postDelete(delete);
        assertEquals(0, preIndex.get());
        assertEquals(0, postIndex.get());
        assertEquals(0, postIndexException.get());
        assertEquals(0, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(0, postDeleteException.get());

        compositeListener.postDelete(delete, new RuntimeException());
        assertEquals(0, preIndex.get());
        assertEquals(0, postIndex.get());
        assertEquals(0, postIndexException.get());
        assertEquals(0, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());

        compositeListener.preDelete(delete);
        assertEquals(0, preIndex.get());
        assertEquals(0, postIndex.get());
        assertEquals(0, postIndexException.get());
        assertEquals(2, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());

        compositeListener.postIndex(index, false);
        assertEquals(0, preIndex.get());
        assertEquals(2, postIndex.get());
        assertEquals(0, postIndexException.get());
        assertEquals(2, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());

        compositeListener.postIndex(index, new RuntimeException());
        assertEquals(0, preIndex.get());
        assertEquals(2, postIndex.get());
        assertEquals(2, postIndexException.get());
        assertEquals(2, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());

        compositeListener.preIndex(index);
        assertEquals(2, preIndex.get());
        assertEquals(2, postIndex.get());
        assertEquals(2, postIndexException.get());
        assertEquals(2, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());
    }
}
