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

package org.elasticsearch.monitor.dump.summary;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.assistedinject.Assisted;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.monitor.dump.Dump;
import org.elasticsearch.monitor.dump.DumpContributionFailedException;
import org.elasticsearch.monitor.dump.DumpContributor;

import java.io.PrintWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Locale;

/**
 *
 */
public class SummaryDumpContributor implements DumpContributor {

    private final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS", Locale.ROOT);
    private final Object formatterLock = new Object();

    public static final String SUMMARY = "summary";

    private final String name;

    @Inject
    public SummaryDumpContributor(@Assisted String name, @Assisted Settings settings) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void contribute(Dump dump) throws DumpContributionFailedException {
        PrintWriter writer = new PrintWriter(dump.createFileWriter("summary.txt"));
        try {
            processHeader(writer, dump.timestamp());
            processCause(writer, dump.cause());
            processThrowables(writer, dump);
        } catch (Exception e) {
            throw new DumpContributionFailedException(getName(), "Failed to generate", e);
        } finally {
            try {
                writer.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private void processHeader(PrintWriter writer, long timestamp) {
        synchronized (formatterLock) {
            writer.println("===== TIME =====");
            writer.println(dateFormat.format(new Date(timestamp)));
            writer.println();
        }
    }

    private void processCause(PrintWriter writer, String cause) {
        writer.println("===== CAUSE =====");
        writer.println(cause);
        writer.println();
    }

    private void processThrowables(PrintWriter writer, Dump dump) {
        writer.println("===== EXCEPTIONS =====");
        Object throwables = dump.context().get("throwables");
        if (throwables == null) {
            return;
        }
        if (throwables instanceof Throwable[]) {
            Throwable[] array = (Throwable[]) throwables;
            for (Throwable t : array) {
                writer.println();
                writer.println("---- Exception ----");
                t.printStackTrace(writer);
            }
        } else if (throwables instanceof Collection) {
            Collection collection = (Collection) throwables;
            for (Object o : collection) {
                Throwable t = (Throwable) o;
                writer.println();
                writer.println("---- Exception ----");
                t.printStackTrace(writer);
            }
        } else {
            throw new DumpContributionFailedException(getName(), "Can't handle throwables type [" + throwables.getClass() + "]");
        }
        writer.println();
    }
}
