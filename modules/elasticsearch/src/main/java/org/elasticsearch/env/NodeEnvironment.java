/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.env;

import org.apache.lucene.store.Lock;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.io.File;
import java.io.IOException;

import static org.elasticsearch.common.settings.ImmutableSettings.Builder.*;

/**
 * @author kimchy (shay.banon)
 */
public class NodeEnvironment extends AbstractComponent {

    private final File nodeFile;

    private final Lock lock;

    public NodeEnvironment(File nodeFile) {
        super(EMPTY_SETTINGS);
        this.nodeFile = nodeFile;
        this.lock = null;
    }

    @Inject public NodeEnvironment(Settings settings, Environment environment) throws IOException {
        super(settings);

        if (!settings.getAsBoolean("node.data", true) || settings.getAsBoolean("node.client", false) ||
                !settings.getAsBoolean("node.master", true)) {
            nodeFile = null;
            lock = null;
            return;
        }

        Lock lock = null;
        File dir = null;
        for (int i = 0; i < 100; i++) {
            dir = new File(new File(environment.workWithClusterFile(), "nodes"), Integer.toString(i));
            if (!dir.exists()) {
                dir.mkdirs();
            }
            try {
                NativeFSLockFactory lockFactory = new NativeFSLockFactory(dir);
                Lock tmpLock = lockFactory.makeLock("node.lock");
                boolean obtained = tmpLock.obtain();
                if (obtained) {
                    lock = tmpLock;
                    break;
                }
            } catch (IOException e) {
                // ignore
            }
        }
        if (lock == null) {
            throw new IOException("Failed to obtain node lock");
        }
        this.lock = lock;
        this.nodeFile = dir;
        if (logger.isDebugEnabled()) {
            logger.debug("using node location [{}]", dir);
        }
    }

    public boolean hasNodeFile() {
        return nodeFile != null && lock != null;
    }

    public File nodeFile() {
        if (nodeFile == null || lock == null) {
            throw new ElasticSearchIllegalStateException("node is not configured to store local location");
        }
        return nodeFile;
    }

    public void close() {
        if (lock != null) {
            try {
                lock.release();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
