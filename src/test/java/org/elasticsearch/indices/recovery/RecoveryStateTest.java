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
package org.elasticsearch.indices.recovery;

import org.elasticsearch.ElasticsearchIllegalStateException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.recovery.RecoveryState.*;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.*;

public class RecoveryStateTest extends ElasticsearchTestCase {

    abstract class Streamer<T extends Streamable> extends Thread {

        private T lastRead;
        final private AtomicBoolean shouldStop;
        final private T source;
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final Version streamVersion;

        Streamer(AtomicBoolean shouldStop, T source) {
            this(shouldStop, source, randomVersion());
        }

        Streamer(AtomicBoolean shouldStop, T source, Version streamVersion) {
            this.shouldStop = shouldStop;
            this.source = source;
            this.streamVersion = streamVersion;
        }

        public T lastRead() throws Throwable {
            Throwable t = error.get();
            if (t != null) {
                throw t;
            }
            return lastRead;
        }

        public T serializeDeserialize() throws IOException {
            BytesStreamOutput out = new BytesStreamOutput();
            source.writeTo(out);
            out.close();
            StreamInput in = new BytesStreamInput(out.bytes());
            T obj = deserialize(in);
            lastRead = obj;
            return obj;
        }

        protected T deserialize(StreamInput in) throws IOException {
            T obj = createObj();
            obj.readFrom(in);
            return obj;
        }

        abstract T createObj();

        @Override
        public void run() {
            try {
                while (shouldStop.get() == false) {
                    serializeDeserialize();
                }
                serializeDeserialize();
            } catch (Throwable t) {
                error.set(t);
            }
        }
    }

    public void testTimers() throws Throwable {
        final Timer timer;
        Streamer<Timer> streamer;
        AtomicBoolean stop = new AtomicBoolean();
        if (randomBoolean()) {
            timer = new Timer();
            streamer = new Streamer<Timer>(stop, timer) {
                @Override
                Timer createObj() {
                    return new Timer();
                }
            };
        } else if (randomBoolean()) {
            timer = new Index();
            streamer = new Streamer<Timer>(stop, timer) {
                @Override
                Timer createObj() {
                    return new Index();
                }
            };
        } else if (randomBoolean()) {
            timer = new Start();
            streamer = new Streamer<Timer>(stop, timer) {
                @Override
                Timer createObj() {
                    return new Start();
                }
            };
        } else {
            timer = new Translog();
            streamer = new Streamer<Timer>(stop, timer) {
                @Override
                Timer createObj() {
                    return new Translog();
                }
            };
        }

        timer.start();
        assertThat(timer.startTime(), greaterThan(0l));
        assertThat(timer.stopTime(), equalTo(0l));
        Timer lastRead = streamer.serializeDeserialize();
        final long time = lastRead.time();
        assertThat(time, lessThanOrEqualTo(timer.time()));
        assertBusy(new Runnable() {
            @Override
            public void run() {
                assertThat("timer timer should progress compared to captured one ", time, lessThan(timer.time()));
            }
        });
        assertThat("captured time shouldn't change", lastRead.time(), equalTo(time));

        if (randomBoolean()) {
            timer.stop();
            assertThat(timer.stopTime(), greaterThanOrEqualTo(timer.startTime()));
            assertThat(timer.time(), equalTo(timer.stopTime() - timer.startTime()));
            lastRead = streamer.serializeDeserialize();
            assertThat(lastRead.startTime(), equalTo(timer.startTime()));
            assertThat(lastRead.time(), equalTo(timer.time()));
            assertThat(lastRead.stopTime(), equalTo(timer.stopTime()));
        }

        timer.reset();
        assertThat(timer.startTime(), equalTo(0l));
        assertThat(timer.time(), equalTo(0l));
        assertThat(timer.stopTime(), equalTo(0l));
        lastRead = streamer.serializeDeserialize();
        assertThat(lastRead.startTime(), equalTo(0l));
        assertThat(lastRead.time(), equalTo(0l));
        assertThat(lastRead.stopTime(), equalTo(0l));

    }

    public void testIndex() throws Throwable {
        File[] files = new File[randomIntBetween(1, 20)];
        ArrayList<File> filesToRecover = new ArrayList<>();
        long totalFileBytes = 0;
        long totalReusedBytes = 0;
        int totalReused = 0;
        for (int i = 0; i < files.length; i++) {
            final int fileLength = randomIntBetween(1, 1000);
            final boolean reused = randomBoolean();
            totalFileBytes += fileLength;
            files[i] = new RecoveryState.File("f_" + i, fileLength, reused);
            if (reused) {
                totalReused++;
                totalReusedBytes += fileLength;
            } else {
                filesToRecover.add(files[i]);
            }
        }

        Collections.shuffle(Arrays.asList(files));
        final RecoveryState.Index index = new RecoveryState.Index();

        if (randomBoolean()) {
            // initialize with some data and then reset
            index.start();
            for (int i = randomIntBetween(0, 10); i > 0; i--) {
                index.addFileDetail("t_" + i, randomIntBetween(1, 100), randomBoolean());
                if (randomBoolean()) {
                    index.addSourceThrottling(randomIntBetween(0, 20));
                }
                if (randomBoolean()) {
                    index.addTargetThrottling(randomIntBetween(0, 20));
                }
            }
            if (randomBoolean()) {
                index.stop();
            }
            index.reset();
        }


        // before we start we must report 0
        assertThat(index.recoveredFilesPercent(), equalTo((float) 0.0));
        assertThat(index.recoveredBytesPercent(), equalTo((float) 0.0));
        assertThat(index.sourceThrottling().nanos(), equalTo(Index.UNKNOWN));
        assertThat(index.targetThrottling().nanos(), equalTo(Index.UNKNOWN));

        index.start();
        for (File file : files) {
            index.addFileDetail(file.name(), file.length(), file.reused());
        }

        logger.info("testing initial information");
        assertThat(index.totalBytes(), equalTo(totalFileBytes));
        assertThat(index.reusedBytes(), equalTo(totalReusedBytes));
        assertThat(index.totalRecoverBytes(), equalTo(totalFileBytes - totalReusedBytes));
        assertThat(index.totalFileCount(), equalTo(files.length));
        assertThat(index.reusedFileCount(), equalTo(totalReused));
        assertThat(index.totalRecoverFiles(), equalTo(filesToRecover.size()));
        assertThat(index.recoveredFileCount(), equalTo(0));
        assertThat(index.recoveredBytes(), equalTo(0l));
        assertThat(index.recoveredFilesPercent(), equalTo(filesToRecover.size() == 0 ? 100.0f : 0.0f));
        assertThat(index.recoveredBytesPercent(), equalTo(filesToRecover.size() == 0 ? 100.0f : 0.0f));


        long bytesToRecover = totalFileBytes - totalReusedBytes;
        boolean completeRecovery = bytesToRecover == 0 || randomBoolean();
        if (completeRecovery == false) {
            bytesToRecover = randomIntBetween(1, (int) bytesToRecover);
            logger.info("performing partial recovery ([{}] bytes of [{}])", bytesToRecover, totalFileBytes - totalReusedBytes);
        }
        AtomicBoolean streamShouldStop = new AtomicBoolean();

        Streamer<Index> backgroundReader = new Streamer<RecoveryState.Index>(streamShouldStop, index) {
            @Override
            Index createObj() {
                return new Index();
            }
        };

        backgroundReader.start();

        long recoveredBytes = 0;
        long sourceThrottling = Index.UNKNOWN;
        long targetThrottling = Index.UNKNOWN;
        while (bytesToRecover > 0) {
            File file = randomFrom(filesToRecover);
            final long toRecover = Math.min(bytesToRecover, randomIntBetween(1, (int) (file.length() - file.recovered())));
            final long throttledOnSource = rarely() ? randomIntBetween(10, 200) : 0;
            index.addSourceThrottling(throttledOnSource);
            if (sourceThrottling == Index.UNKNOWN) {
                sourceThrottling = throttledOnSource;
            } else {
                sourceThrottling += throttledOnSource;
            }
            index.addRecoveredBytesToFile(file.name(), toRecover);
            file.addRecoveredBytes(toRecover);
            final long throttledOnTarget = rarely() ? randomIntBetween(10, 200) : 0;
            if (targetThrottling == Index.UNKNOWN) {
                targetThrottling = throttledOnTarget;
            } else {
                targetThrottling += throttledOnTarget;
            }
            index.addTargetThrottling(throttledOnTarget);
            bytesToRecover -= toRecover;
            recoveredBytes += toRecover;
            if (file.reused() || file.fullyRecovered()) {
                filesToRecover.remove(file);
            }
        }

        if (completeRecovery) {
            assertThat(filesToRecover.size(), equalTo(0));
            index.stop();
            assertThat(index.time(), equalTo(index.stopTime() - index.startTime()));
            assertThat(index.time(), equalTo(index.stopTime() - index.startTime()));
        }

        logger.info("testing serialized information");
        streamShouldStop.set(true);
        backgroundReader.join();
        final Index lastRead = backgroundReader.lastRead();
        assertThat(lastRead.fileDetails().toArray(), arrayContainingInAnyOrder(index.fileDetails().toArray()));
        assertThat(lastRead.startTime(), equalTo(index.startTime()));
        if (completeRecovery) {
            assertThat(lastRead.time(), equalTo(index.time()));
        } else {
            assertThat(lastRead.time(), lessThanOrEqualTo(index.time()));
        }
        assertThat(lastRead.stopTime(), equalTo(index.stopTime()));
        assertThat(lastRead.targetThrottling(), equalTo(index.targetThrottling()));
        assertThat(lastRead.sourceThrottling(), equalTo(index.sourceThrottling()));

        logger.info("testing post recovery");
        assertThat(index.totalBytes(), equalTo(totalFileBytes));
        assertThat(index.reusedBytes(), equalTo(totalReusedBytes));
        assertThat(index.totalRecoverBytes(), equalTo(totalFileBytes - totalReusedBytes));
        assertThat(index.totalFileCount(), equalTo(files.length));
        assertThat(index.reusedFileCount(), equalTo(totalReused));
        assertThat(index.totalRecoverFiles(), equalTo(files.length - totalReused));
        assertThat(index.recoveredFileCount(), equalTo(index.totalRecoverFiles() - filesToRecover.size()));
        assertThat(index.recoveredBytes(), equalTo(recoveredBytes));
        assertThat(index.targetThrottling().nanos(), equalTo(targetThrottling));
        assertThat(index.sourceThrottling().nanos(), equalTo(sourceThrottling));
        if (index.totalRecoverFiles() == 0) {
            assertThat((double) index.recoveredFilesPercent(), equalTo(100.0));
            assertThat((double) index.recoveredBytesPercent(), equalTo(100.0));
        } else {
            assertThat((double) index.recoveredFilesPercent(), closeTo(100.0 * index.recoveredFileCount() / index.totalRecoverFiles(), 0.1));
            assertThat((double) index.recoveredBytesPercent(), closeTo(100.0 * index.recoveredBytes() / index.totalRecoverBytes(), 0.1));
        }
    }

    public void testStageSequenceEnforcement() {
        final DiscoveryNode discoveryNode = new DiscoveryNode("1", DummyTransportAddress.INSTANCE, Version.CURRENT);
        Stage[] stages = Stage.values();
        int i = randomIntBetween(0, stages.length - 1);
        int j;
        do {
            j = randomIntBetween(0, stages.length - 1);
        } while (j == i);
        Stage t = stages[i];
        stages[i] = stages[j];
        stages[j] = t;
        try {
            RecoveryState state = new RecoveryState(new ShardId("bla", 0), randomBoolean(), randomFrom(Type.values()), discoveryNode, discoveryNode);
            for (Stage stage : stages) {
                state.setStage(stage);
            }
            fail("succeeded in performing the illegal sequence [" + Strings.arrayToCommaDelimitedString(stages) + "]");
        } catch (ElasticsearchIllegalStateException e) {
            // cool
        }

        // but reset should be always possible.
        stages = Stage.values();
        i = randomIntBetween(1, stages.length - 1);
        ArrayList<Stage> list = new ArrayList<>(Arrays.asList(Arrays.copyOfRange(stages, 0, i)));
        list.addAll(Arrays.asList(stages));
        RecoveryState state = new RecoveryState(new ShardId("bla", 0), randomBoolean(), randomFrom(Type.values()), discoveryNode, discoveryNode);
        for (Stage stage : list) {
            state.setStage(stage);
        }

        assertThat(state.getStage(), equalTo(Stage.DONE));
    }

    public void testTranslog() throws Throwable {
        final Translog translog = new Translog();
        AtomicBoolean stop = new AtomicBoolean();
        Streamer<Translog> streamer = new Streamer<Translog>(stop, translog) {
            @Override
            Translog createObj() {
                return new Translog();
            }
        };

        // we don't need to test the time aspect, it's done in the timer test
        translog.start();
        assertThat(translog.currentTranslogOperations(), equalTo(0));
        streamer.start();
        // force one
        streamer.serializeDeserialize();
        int ops = 0;
        for (int i = scaledRandomIntBetween(10, 200); i > 0; i--) {
            for (int j = randomIntBetween(1, 10); j > 0; j--) {
                ops++;
                translog.incrementTranslogOperations();
            }
            assertThat(translog.currentTranslogOperations(), equalTo(ops));
            assertThat(streamer.lastRead().currentTranslogOperations(), greaterThanOrEqualTo(0));
            assertThat(streamer.lastRead().currentTranslogOperations(), lessThanOrEqualTo(ops));
        }

        boolean stopped = false;
        if (randomBoolean()) {
            translog.stop();
            stopped = true;
        }

        if (randomBoolean()) {
            translog.reset();
            ops = 0;
            assertThat(translog.currentTranslogOperations(), equalTo(0));
        }

        stop.set(true);
        streamer.join();
        final Translog lastRead = streamer.lastRead();
        assertThat(lastRead.currentTranslogOperations(), equalTo(ops));
        assertThat(lastRead.startTime(), equalTo(translog.startTime()));
        assertThat(lastRead.stopTime(), equalTo(translog.stopTime()));

        if (stopped) {
            assertThat(lastRead.time(), equalTo(translog.time()));
        } else {
            assertThat(lastRead.time(), lessThanOrEqualTo(translog.time()));
        }
    }

    public void testStart() throws IOException {
        final Start start = new Start();
        AtomicBoolean stop = new AtomicBoolean();
        Streamer<Start> streamer = new Streamer<Start>(stop, start) {
            @Override
            Start createObj() {
                return new Start();
            }
        };

        // we don't need to test the time aspect, it's done in the timer test
        start.start();
        assertThat(start.checkIndexTime(), equalTo(0l));
        // force one
        Start lastRead = streamer.serializeDeserialize();
        assertThat(lastRead.checkIndexTime(), equalTo(0l));

        long took = randomLong();
        if (took < 0) {
            took = -took;
            took = Math.max(0l, took);

        }
        start.checkIndexTime(took);
        assertThat(start.checkIndexTime(), equalTo(took));

        boolean stopped = false;
        if (randomBoolean()) {
            start.stop();
            stopped = true;
        }

        if (randomBoolean()) {
            start.reset();
            took = 0;
            assertThat(start.checkIndexTime(), equalTo(took));
        }

        lastRead = streamer.serializeDeserialize();
        assertThat(lastRead.checkIndexTime(), equalTo(took));
        assertThat(lastRead.startTime(), equalTo(start.startTime()));
        assertThat(lastRead.stopTime(), equalTo(start.stopTime()));

        if (stopped) {
            assertThat(lastRead.time(), equalTo(start.time()));
        } else {
            assertThat(lastRead.time(), lessThanOrEqualTo(start.time()));
        }
    }
}
