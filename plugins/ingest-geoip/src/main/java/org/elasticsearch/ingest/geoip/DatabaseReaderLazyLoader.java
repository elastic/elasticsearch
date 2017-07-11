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
package org.elasticsearch.ingest.geoip;

import com.maxmind.geoip2.DatabaseReader;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.logging.Loggers;

import java.io.Closeable;
import java.io.IOException;

/**
 * Facilitates lazy loading of the database reader, so that when the geoip plugin is installed, but not used,
 * no memory is being wasted on the database reader.
 */
final class DatabaseReaderLazyLoader implements Closeable {

    private static final Logger LOGGER = Loggers.getLogger(DatabaseReaderLazyLoader.class);

    private final String databaseFileName;
    private final CheckedSupplier<DatabaseReader, IOException> loader;
    // package protected for testing only:
    final SetOnce<DatabaseReader> databaseReader;

    DatabaseReaderLazyLoader(String databaseFileName, CheckedSupplier<DatabaseReader, IOException> loader) {
        this.databaseFileName = databaseFileName;
        this.loader = loader;
        this.databaseReader = new SetOnce<>();
    }

    synchronized DatabaseReader get() throws IOException {
        if (databaseReader.get() == null) {
            databaseReader.set(loader.get());
            LOGGER.debug("Loaded [{}] geoip database", databaseFileName);
        }
        return databaseReader.get();
    }

    @Override
    public synchronized void close() throws IOException {
        IOUtils.close(databaseReader.get());
    }
}
