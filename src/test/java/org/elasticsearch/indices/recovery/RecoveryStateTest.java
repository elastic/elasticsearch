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

import org.elasticsearch.common.io.stream.BytesStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.indices.recovery.RecoveryState.File;
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

        Streamer(AtomicBoolean shouldStop, T source) {
            this.shouldStop = shouldStop;
            this.source = source;
        }

        void serializeDeserialize() throws IOException {
            BytesStreamOutput out = new BytesStreamOutput();
            source.writeTo(out);
            out.close();
            StreamInput in = new BytesStreamInput(out.bytes());
            lastRead = deserialize(in);
        }

        abstract T deserialize(StreamInput in) throws IOException;

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

    public void testIndex() throws Exception {
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
        final long startTime = System.currentTimeMillis();
        index.startTime(startTime);
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
        assertThat(index.recoverdFilesPercent(), equalTo((float) 0.0));
        assertThat(index.startTime(), equalTo(startTime));


        long bytesToRecover = totalFileBytes - totalReusedBytes;
        boolean completeRecovery = bytesToRecover == 0 || randomBoolean();
        if (completeRecovery == false) {
            bytesToRecover = randomIntBetween(1, (int) bytesToRecover);
            logger.info("performing partial recovery ([{}] bytes of [{}])", bytesToRecover, totalFileBytes - totalReusedBytes);
        }
        AtomicBoolean streamShouldStop = new AtomicBoolean();

        Streamer<RecoveryState.Index> backgroundReader = new Streamer<RecoveryState.Index>(streamShouldStop, index) {
            @Override
            RecoveryState.Index deserialize(StreamInput in) throws IOException {
                return RecoveryState.Index.readIndex(in);
            }
        };

        backgroundReader.start();

        long recoveredBytes = 0;
        while (bytesToRecover > 0) {
            File file = randomFrom(filesToRecover);
            long toRecover = Math.min(bytesToRecover, randomIntBetween(1, (int) (file.length() - file.recovered())));
            index.addRecoveredBytesToFile(file.name(), toRecover);
            file.addRecoveredBytes(toRecover);
            bytesToRecover -= toRecover;
            recoveredBytes += toRecover;
            if (file.reused() || file.fullyRecovered()) {
                filesToRecover.remove(file);
            }
        }

        if (completeRecovery) {
            assertThat(filesToRecover.size(), equalTo(0));
            long time = System.currentTimeMillis() - startTime;
            index.time(time);
            assertThat(index.time(), equalTo(time));
        }

        logger.info("testing serialized information");
        streamShouldStop.set(true);
        backgroundReader.join();
        assertThat(backgroundReader.lastRead.fileDetails().toArray(), arrayContainingInAnyOrder(index.fileDetails().toArray()));
        assertThat(backgroundReader.lastRead.startTime(), equalTo(index.startTime()));
        assertThat(backgroundReader.lastRead.time(), equalTo(index.time()));

        logger.info("testing post recovery");
        assertThat(index.totalBytes(), equalTo(totalFileBytes));
        assertThat(index.reusedBytes(), equalTo(totalReusedBytes));
        assertThat(index.totalRecoverBytes(), equalTo(totalFileBytes - totalReusedBytes));
        assertThat(index.totalFileCount(), equalTo(files.length));
        assertThat(index.reusedFileCount(), equalTo(totalReused));
        assertThat(index.totalRecoverFiles(), equalTo(files.length - totalReused));
        assertThat(index.recoveredFileCount(), equalTo(index.totalRecoverFiles() - filesToRecover.size()));
        assertThat(index.recoveredBytes(), equalTo(recoveredBytes));
        if (index.totalRecoverFiles() == 0) {
            assertThat((double) index.recoverdFilesPercent(), equalTo(0.0));
        } else {
            assertThat((double) index.recoverdFilesPercent(), closeTo(100.0 * index.recoveredFileCount() / index.totalRecoverFiles(), 0.1));
        }
        assertThat(index.startTime(), equalTo(startTime));
    }

}
