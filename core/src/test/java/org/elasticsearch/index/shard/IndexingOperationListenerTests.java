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
            public void preOperation(Engine.Operation operation) {
                switch (operation.operationType()) {
                    case INDEX:
                        preIndex.incrementAndGet();
                        break;
                    case DELETE:
                        preDelete.incrementAndGet();
                        break;
                }
            }

            @Override
            public void postOperation(Engine.Operation operation) {
                switch (operation.operationType()) {
                    case INDEX:
                        postIndex.incrementAndGet();
                        break;
                    case DELETE:
                        postDelete.incrementAndGet();
                        break;
                }
            }

            @Override
            public void postOperation(Engine.Operation operation, Exception ex) {
                switch (operation.operationType()) {
                    case INDEX:
                        postIndexException.incrementAndGet();
                        break;
                    case DELETE:
                        postDeleteException.incrementAndGet();
                        break;
                }
            }
        };

        IndexingOperationListener throwingListener = new IndexingOperationListener() {
            @Override
            public void preOperation(Engine.Operation operation) {
                throw new RuntimeException();
            }

            @Override
            public void postOperation(Engine.Operation operation) {
                throw new RuntimeException();
            }

            @Override
            public void postOperation(Engine.Operation operation, Exception ex) {
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
        compositeListener.postOperation(delete);
        assertEquals(0, preIndex.get());
        assertEquals(0, postIndex.get());
        assertEquals(0, postIndexException.get());
        assertEquals(0, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(0, postDeleteException.get());

        compositeListener.postOperation(delete, new RuntimeException());
        assertEquals(0, preIndex.get());
        assertEquals(0, postIndex.get());
        assertEquals(0, postIndexException.get());
        assertEquals(0, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());

        compositeListener.preOperation(delete);
        assertEquals(0, preIndex.get());
        assertEquals(0, postIndex.get());
        assertEquals(0, postIndexException.get());
        assertEquals(2, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());

        compositeListener.postOperation(index);
        assertEquals(0, preIndex.get());
        assertEquals(2, postIndex.get());
        assertEquals(0, postIndexException.get());
        assertEquals(2, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());

        compositeListener.postOperation(index, new RuntimeException());
        assertEquals(0, preIndex.get());
        assertEquals(2, postIndex.get());
        assertEquals(2, postIndexException.get());
        assertEquals(2, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());

        compositeListener.preOperation(index);
        assertEquals(2, preIndex.get());
        assertEquals(2, postIndex.get());
        assertEquals(2, postIndexException.get());
        assertEquals(2, preDelete.get());
        assertEquals(2, postDelete.get());
        assertEquals(2, postDeleteException.get());
    }
}
