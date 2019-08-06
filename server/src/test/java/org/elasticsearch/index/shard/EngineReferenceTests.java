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

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Phaser;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;

public class EngineReferenceTests extends ESTestCase {

    public void testSimple() throws Exception {
        final EngineReference engineReference = new EngineReference();
        assertNull(engineReference.get());
        List<Engine> engines = new ArrayList<>();
        int iterations = randomIntBetween(0, 10);
        for (int i = 0; i < iterations; i++) {
            Engine oldEngine = engineReference.get();
            Engine newEngine = mock(Engine.class);
            engineReference.swapReference(newEngine);
            engines.add(newEngine);
            assertSame(newEngine, engineReference.get());
            if (oldEngine != null) {
                verify(oldEngine, times(1)).close();
            }
        }
        if (randomBoolean()) {
            engineReference.flushAndClose();
            if (engines.isEmpty() == false) {
                Engine flushedEngine = engines.remove(engines.size() - 1);
                verify(flushedEngine, times(1)).flushAndClose();
            }
        } else {
            engineReference.close();
        }
        assertNull(engineReference.get());
        for (Engine engine : engines) {
            verify(engine, times(1)).close();
        }
        Engine newEngine = mock(Engine.class);
        AlreadyClosedException ace = expectThrows(AlreadyClosedException.class, () -> engineReference.swapReference(newEngine));
        assertThat(ace.getMessage(), containsString("engine reference was closed"));
        verifyZeroInteractions(newEngine);
    }

    public void testSwapAndCloseConcurrently() throws Exception {
        final EngineReference engineReference = new EngineReference();
        final Phaser phaser = new Phaser(2);
        Thread closeThread = new Thread(() -> {
            try {
                phaser.arriveAndAwaitAdvance();
                engineReference.close();
            } catch (IOException e) {
                throw new AssertionError(e);
            }
        });
        closeThread.start();
        phaser.arriveAndAwaitAdvance();
        List<Engine> engines = new ArrayList<>();
        int iterations = randomIntBetween(0, 100);
        for (int i = 0; i < iterations; i++) {
            Engine newEngine = mock(Engine.class);
            try {
                Engine oldEngine = engineReference.get();
                engineReference.swapReference(newEngine);
                assertThat(engineReference.get(), either(sameInstance(newEngine)).or(nullValue()));
                if (oldEngine != null) {
                    verify(oldEngine, times(1)).close();
                }
                engines.add(newEngine);
            } catch (AlreadyClosedException ace) {
                verifyZeroInteractions(newEngine);
                assertThat(ace.getMessage(), containsString("engine reference was closed"));
            }
        }
        closeThread.join();
        assertNull(engineReference.get());
        for (Engine engine : engines) {
            verify(engine, times(1)).close();
        }
        Engine newEngine = mock(Engine.class);
        AlreadyClosedException ace = expectThrows(AlreadyClosedException.class, () -> engineReference.swapReference(newEngine));
        assertThat(ace.getMessage(), containsString("engine reference was closed"));
        verifyZeroInteractions(newEngine);
    }
}
