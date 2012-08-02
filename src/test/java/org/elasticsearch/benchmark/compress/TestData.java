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

package org.elasticsearch.benchmark.compress;

import org.elasticsearch.common.compress.bzip2.CBZip2InputStream;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.Date;

/**
 */
public class TestData {

    private BufferedReader reader;

    private String line;
    private String id;
    private String type;
    private String text;

    private long totalSize;

    public TestData() throws IOException {
        URL url = new URL("http://downloads.dbpedia.org/3.0/en/longabstract_en.nt.bz2");
        BufferedInputStream stream = new BufferedInputStream(url.openStream());
        // read two bytes for the header...
        stream.read();
        stream.read();
        reader = new BufferedReader(new InputStreamReader(new CBZip2InputStream(stream)));
    }

    public long getTotalSize() {
        return totalSize;
    }

    public boolean next() throws Exception {
        line = reader.readLine();
        if (line == null) {
            return false;
        }
        totalSize += line.length();
        int endId = line.indexOf(' ');
        id = line.substring(0, endId);
        int endType = line.indexOf(' ', endId + 1);
        type = line.substring(endId + 1, endType);
        text = line.substring(endType + 1);
        return true;
    }

    public String currentText() {
        return text;
    }

    /**
     */
    public XContentBuilder current(XContentBuilder builder) throws Exception {
        builder.startObject();
        builder.field("id", id);
        builder.field("type", type);
        builder.field("text", text);
        builder.field("time", new Date());
        builder.endObject();
        return builder;
    }
}
