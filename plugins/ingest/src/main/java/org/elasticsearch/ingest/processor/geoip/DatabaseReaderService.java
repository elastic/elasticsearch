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

package org.elasticsearch.ingest.processor.geoip;

import com.maxmind.geoip2.DatabaseReader;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

final class DatabaseReaderService implements Closeable {

    private final Map<String, DatabaseReader> databaseReaders = new HashMap<>();

    synchronized DatabaseReader getOrCreateDatabaseReader(String key, InputStream inputStream) throws IOException {
        DatabaseReader databaseReader = databaseReaders.get(key);
        if (databaseReader != null) {
            return databaseReader;
        }

        databaseReader = new DatabaseReader.Builder(inputStream).build();
        databaseReaders.put(key, databaseReader);
        return databaseReader;
    }

    @Override
    public void close() throws IOException {
        for (DatabaseReader databaseReader : databaseReaders.values()) {
            databaseReader.close();
        }
    }
}
