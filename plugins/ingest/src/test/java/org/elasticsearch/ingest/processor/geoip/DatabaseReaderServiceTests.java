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
import org.elasticsearch.test.ESTestCase;
import static org.hamcrest.Matchers.*;

import java.io.InputStream;

public class DatabaseReaderServiceTests extends ESTestCase {

    public void testLookup() throws Exception {
        InputStream database = DatabaseReaderServiceTests.class.getResourceAsStream("/GeoLite2-City.mmdb");

        DatabaseReaderService service = new DatabaseReaderService();
        DatabaseReader instance = service.getOrCreateDatabaseReader("key1", database);
        assertThat(service.getOrCreateDatabaseReader("key1", database), equalTo(instance));

        database = DatabaseReaderServiceTests.class.getResourceAsStream("/GeoLite2-City.mmdb");
        assertThat(service.getOrCreateDatabaseReader("key2", database), not(equalTo(instance)));
    }

}
