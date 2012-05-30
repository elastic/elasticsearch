/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Contributors:  Mathias Bogaert

package org.elasticsearch.common.logging.log4j;

import java.io.File;

import org.apache.log4j.helpers.LogLog;

/**
 * Watcher of files. On change of any file, call the method {@link #doOnChange()}
 */
public abstract class FilesWatchdog extends Thread {

    public static final long DEFAULT_DELAY = 10000;

    private long delay = DEFAULT_DELAY;

    private File[] files;

    private long[] lastModifs;

    private boolean warnedAlready = false;

    protected FilesWatchdog(File... files) {
        super("FileWatchdog");
        this.files = files;
        this.lastModifs = new long[files.length];
        setDaemon(true);
        checkAnyChanged();
    }

    public void setDelay(long delay) {
        this.delay = delay;
    }

    abstract protected void doOnChange();

    private boolean checkAnyChanged() {
        boolean changed = false;

        for (int i = 0; i < files.length; i++) {
            long l = checkChanged(files[i], lastModifs[i]);
            if (l > 0) {
                lastModifs[i] = l;
                changed = true;
            }
        }

        return changed;
    }

    private long checkChanged(File file, long lastModif) {
        if (file.exists()) {
            long l = file.lastModified();
            if (l > lastModif) {
                warnedAlready = false;
                return l;
            }
        } else {
            if (!warnedAlready) {
                LogLog.debug("[" + file.getAbsolutePath() + "] does not exist.");
                warnedAlready = true;
            }
        }
        return 0;
    }

    public void run() {
        while (true) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                // no interruption expected
            }
            if (checkAnyChanged()) {
                doOnChange();
            }
        }
    }
}
